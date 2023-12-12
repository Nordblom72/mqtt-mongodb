#!/usr/local/bin node

const { dbHandler } = require('./src/db');
const mqtt = require('mqtt');

const numOfDecimals = 5;
let seconds;
let minutes;
let deviceRoot="NS1PWR/p1ib/";

let stateCtrl = {
  ongoing: false,
  gotFullMeasPeriod: false,
  starting: true,
  sendToDb: false,
  isFirstMeasPeriod: true
}

const pingDb = async () => {
  await dbHandler('ping', {})
  .then(function(acknowledged) {
    if (!acknowledged) {
      console.log("ERROR: DB ping check failed...!");
    }
  })
};

const convertFromUtcToLocalDate = (utcDateObj) => {
  const offset = utcDateObj.getTimezoneOffset();
  return new Date(utcDateObj.getTime() - offset * 60000);
}
const monthsAsTextList = ['January', 'February', 'Mars', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const numDaysInMonth = (year, month) => convertFromUtcToLocalDate(new Date(year, month, 0)).getDate();

const setUpdatePeriodMinutes = () => {
  const validNumbers = [2,3,4,5,6,10,12,15,20,30]; // Number of DB updates per hour
  if (`${process.env.MONGODB_UPDATE_FREQUECY}` != 'undefined' && typeof(parseInt(`${process.env.MONGODB_UPDATE_FREQUECY}`)) === 'number' ) {
    if (validNumbers.includes(parseInt(`${process.env.MONGODB_UPDATE_FREQUECY}`))) {
      console.log("Desired update frequency towards Mongo DB is ", parseInt(`${process.env.MONGODB_UPDATE_FREQUECY}`));
      return (60/parseInt(`${process.env.MONGODB_UPDATE_FREQUECY}`));
    } else {
      console.log("WARNING: Invalid update frequency", `${process.env.MONGODB_UPDATE_FREQUECY}`);
      console.log("Valid values for update frequency are: ", validNumbers);
    }
  }
  console.log("Configuring default update frequency towards Mongo DB to 12.");
  return (5);
}

console.log("Server started: ", convertFromUtcToLocalDate(new Date()));
const updatePeriod = setUpdatePeriodMinutes();
console.log("Database update period is set to every ", updatePeriod, " minutes.");

// The p1IB sends measurement data every 10 seconds
// DB will be updated as defined by updatePeriod (i.e. every updatePerion minutes)
// The measurement may start at any given time within the hour and data will be stored
// in DB as defined by updatePeriod.
let measurement = {
  date: null,
  startValueImport: 0.0,
  startValueExport: 0.0,
  startOfHour: {
    impValue: 0.0,
    expValue: 0.0,
    prevImpValue: 0.0,
    prevExpValue: 0.0,
    date: null,
    prevDate: null
  },
  sendToDbObj: {
    date: null,
    startImpVal: 0.0,
    stopImpVal: 0.0,
    startExpVal: 0.0,
    stopExpVal: 0.0
  },
  isStartOfHour: false,
}

const getMeasCacheinDb = async (date, hour) => {
  if (date.includes('T')) {
    date = date.split('T')[0];
  }
  let rsp = [null, null, null];
  return await dbHandler('get', { type: 'measCache' })
  .then(function(value) {
    if (value && value !== null) {
      try {
        if (value.date.split('T')[0] === date && parseInt(value.hour) === parseInt(hour)) {
          if ( typeof(value.impVal) === 'number' && typeof(value.expVal) === 'number' ) {
            rsp = [value.date, value.impVal,value.expVal];
          }
        }
      }
      catch (error) {
        console.log("ERROR: Failed parsing response data from DB for measCache document!");
        console.log("ERROR: Got rsp: ", value);
        console.log("ERROR: ", error);
        return(rsp);
      }
    }
    return(rsp);
  });
}

const recoverStartData = async (date) => {
  // Validate start of hour
  [hour, ...rest] = date.split('T')[1].split(':');
  if (measurement.startOfHour.date === null) {
    let impVal, expVal = null;
    return await getMeasCacheinDb(date, hour)
    .then(function(value) {
      [saveDate, impVal, expVal] =  value;
      if (impVal !== null && expVal !== null) {
        measurement.startOfHour.date = saveDate;
        measurement.startOfHour.impValue = impVal;
        measurement.startOfHour.expValue = expVal;
        console.log("Succesfully restored hourly start-up data.");
        return (true);
      } else {
        console.log("Failed to restore hourly start-up data. Data not available.");
        return (false);
      }
    });
  }
  return (false);
}

if (stateCtrl.starting) {
  recoverStartData(convertFromUtcToLocalDate(new Date()).toISOString())
  .then(function(value) {
    stateCtrl.starting = false;
    return value;
  });
}

const clientId = `mqtt_${Math.random().toString(16).slice(3)}`;
const conData = {
  clientId,
  clean: true,
  connectTimeout: 4000,
  reconnectPeriod: 10000,
  username: `${process.env.MQTT_USR}`,
  password: `${process.env.MQTT_PSW}`
}
//const client = mqtt.connect("mqtt://test.mosquitto.org");
const client = mqtt.connect("mqtt://localhost:1883", conData);

client.on('error', (error) => {
  console.error('MQTT connection failed!', error)
});

client.on('reconnect', (error) => {
  console.error('MQTT reconnect failed!', error)
});

const topics = {
  'NS1PWR/p1ib/p1ib_clock_date/state': {qos: 1},
  'NS1PWR/p1ib/p1ib_h_active_imp_q1_q4/state': {qos: 1},
  'NS1PWR/p1ib/p1ib_h_active_exp_q2_q3/state': {qos: 1}
};

client.on("connect", () => {
  console.log("MQTT Client Connected with id: ", clientId);
  client.subscribe(topics, (err) => {
    if (err) {
      console.log(err);
    }
  });
});


client.on("message", (topic, message, packet) => {
  //let key=topic.replace(deviceRoot,'');
  if (!stateCtrl.starting) {
    // Date + Time
    if (topic === 'NS1PWR/p1ib/p1ib_clock_date/state') {
      let date = convertFromUtcToLocalDate(new Date(message.toString())).toISOString(); // message is Buffer
      measurement.date = date;  // ... save the date for each burst (i.e. every 10th second) ?
      minutes = message.toString().split(':')[1];
      seconds = message.toString().split(':')[2].split('Z')[0];

      // 1st meas burst of hour?
      if (minutes == 0  && (seconds < 8)) { // start of hour. 1st meas period
        measurement.startOfHour.prevDate = measurement.startOfHour.date;
        measurement.startOfHour.date = date;
      }
      // New Measuerement period start?
      if ((minutes%updatePeriod === 0) && (seconds < 8)) {
        stateCtrl.ongoing = true;
        console.log("--------- New measurement period started: ", measurement.date);
        stateCtrl.sendToDb = true;
      }
    // Import value  
    } else if (stateCtrl.ongoing && topic === 'NS1PWR/p1ib/p1ib_h_active_imp_q1_q4/state') {
      // Last measurement burst for ongoing period?
      if (stateCtrl.sendToDb) {
        measurement.sendToDbObj.startImpVal  = measurement.startValueImport; // Save startValueImport now as it will be overwtritten in next step
        measurement.sendToDbObj.stopImpVal   = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
      }
      // 1st meas burst of hour?
      if (minutes == 0  && (seconds < 8)) {
        measurement.startOfHour.prevImpValue = measurement.startOfHour.impValue; // Back-up before overwriting
        measurement.startOfHour.impValue = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
      }
      // New Measuerement period start?
      if ((minutes%updatePeriod === 0) && (seconds < 8)) {
        measurement.startValueImport = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
      }

    // Export value
    } else if (stateCtrl.ongoing && topic === 'NS1PWR/p1ib/p1ib_h_active_exp_q2_q3/state') {
      // Last measurement burst for ongoing period?
      if (stateCtrl.sendToDb) {
          measurement.sendToDbObj.startExpVal = measurement.startValueExport; // Save startValueExport now as it will be overwtritten in next step
          measurement.sendToDbObj.stopExpVal = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
          stateCtrl.gotFullMeasPeriod = true;
          stateCtrl.sendToDb = false;
      }
      // 1st meas burst of hour?
      if (minutes == 0  && (seconds < 8)) {
        measurement.isStartOfHour = true;
        measurement.startOfHour.prevExpValue = measurement.startOfHour.expValue; //Back-up before overwiriting
        measurement.startOfHour.expValue = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
      }
      // New Measuerement period start?
      if ((minutes%updatePeriod === 0) && (seconds < 8)) {
        measurement.startValueExport = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
      }
    }

    if (stateCtrl.gotFullMeasPeriod) {
      stateCtrl.gotFullMeasPeriod = false;
      if (!(measurement.sendToDbObj.startImpVal == 0 || measurement.sendToDbObj.startExpVal == 0) || !stateCtrl.isFirstMeasPeriod) {
        updateDb(JSON.parse(JSON.stringify({
              date:             measurement.date,
              startValueImport: measurement.sendToDbObj.startImpVal,
              startValueExport: measurement.sendToDbObj.startExpVal,
              stopValueImport:  measurement.sendToDbObj.stopImpVal,
              stopValueExport:  measurement.sendToDbObj.stopExpVal,
              startOfHour:      measurement.startOfHour,
              isStartOfHour:    measurement.isStartOfHour
        }))); // Send a copy of the measurement Object to updateDb ...
      }else {
        console.log("SKIPPED UPDATE OF DB ");
        stateCtrl.isFirstMeasPeriod = false;
      }
      // Clear
      measurement.sendToDbObj.date = null;
      measurement.sendToDbObj.startImpVal = 0.0;
      measurement.sendToDbObj.startExpVal = 0.0;
      measurement.sendToDbObj.stopImpVal = 0.0;
      measurement.sendToDbObj.stopExpVal = 0.0;
      measurement.isStartOfHour = false;
    }
  }
  //client.end(); 
});

const createDbDayObject = (dateStr) => {
  hrObj = {
    produced: 0.0,
    exported: 0.0,
    imported: 0.0,
    temperature: null,
    isComplete: false,
    impExpIsComplete: false
  }
  dayObj = {
    date: dateStr.split('T')[0],
    produced: 0.0,
    exported: 0.0,
    imported: 0.0,
    impExpIsComplete: false,
    hours: []
  }
  for(let i=0; i<24; i++) {
    dayObj.hours[i] =  Object.assign({}, hrObj);
  }
  return dayObj;
}

const createDbMonthObj = (dateStr) => {
  [year, month, day, ...rest] = dateStr.split('T')[0].split('-');

  dbMonthObj = {
    year: parseInt(year),
    monthName: monthsAsTextList[month-1], // monthNr  0-11
    monthlyPwrData: {
      produced: 0.0,
      exported: 0.0,
      imported: 0.0,
      impExpIsComplete: false
    },
    dailyPwrData: []
  }
  // If it isn't 1:th of month, then create day objects until 'yesterday'
  const dayObj = createDbDayObject('');
  for(let i=0; i<(day - 1); i++) { // Until yesterday
    dbMonthObj.dailyPwrData[i] = Object.assign({}, dayObj);
    dbMonthObj.dailyPwrData[i].date = convertFromUtcToLocalDate(new Date(`${year} ${month} ${i+1}`)).toISOString().split('T')[0];
  }
  return dbMonthObj;
}

const updateMeasCacheinDb = async (date, hour, impVal, expVal) => {
  const newCacheData = { type: 'measCache', date: date, hour: hour, impVal: impVal, expVal: expVal };
  let cache = await dbHandler('get', { type: 'measCache' })
  .then(function(value) {
    return value;
  });
  if (cache !== null) {
    let setQuery = { identifier: { _id: cache._id }, data: { $set: newCacheData } };
    await dbHandler('update', setQuery)
    .then((acknowledged) => {
      if (!acknowledged) {
        console.log("ERROR:   updateMeasCacheinDb(), FAILED to update DB with hourly start-up data!");
      } else {console.log("DB updated with new measueremnt cache data.")}
    });
  } else {
    await dbHandler('create', newCacheData)
    .then(function(acknowledged) {
      if (!acknowledged) { 
        console.log("ERROR:   updateMeasCacheinDb(), FAILED to create new measCache object in DB!");
      } else {console.log("DB updated/created with new measueremnt cache object.")}
    });
  }
}

const updateDb = async (measurement) => {
  //console.log(JSON.stringify(measurement, null, 2));
  let hour, minute, rest;
  let year, month, day;
  let impVal, expVal = 0.0;
  let date;
  let isFullHour = false;

  if (measurement.startOfHour.prevDate !== null && measurement.isStartOfHour) {
    date = measurement.startOfHour.prevDate; // It s start of hour, but the meas data is for last meas period of previous hour
  } else {
    date = measurement.date; // Ackumulated update
  }

  // Save start of hour data in DB as backup ?
  if (measurement.isStartOfHour) {
    [hr, ...rest] = measurement.startOfHour.date.split('T')[1].split(':');
    await updateMeasCacheinDb(measurement.startOfHour.date, parseInt(hr), parseFloat(measurement.startOfHour.impValue), parseFloat(measurement.startOfHour.expValue)); 
  }

  [hour, minute, ...rest] = date.split('T')[1].split(':');
  [year, month, day, ...rest] = date.split('T')[0].split('-');

  // Calculate diff values for impVal & expVal
  if (measurement.startOfHour.prevDate !== null && measurement.isStartOfHour) {
      isFullHour = true;
      impVal = parseFloat(measurement.startOfHour.impValue - measurement.startOfHour.prevImpValue).toFixed(numOfDecimals);
      expVal = parseFloat(measurement.startOfHour.expValue - measurement.startOfHour.prevExpValue).toFixed(numOfDecimals);
  } else {
    impVal = parseFloat(measurement.stopValueImport - measurement.startValueImport).toFixed(numOfDecimals);
    expVal = parseFloat(measurement.stopValueExport - measurement.startValueExport).toFixed(numOfDecimals);
  }

  let monthName = monthsAsTextList[month-1]; // monthNr  0-11
  let query = { year: parseInt(year), monthName: monthName };
  let dbMonthObj = await dbHandler('get', query)
                  .then(function(value) {
                    return value;
                  });
  
  if (dbMonthObj === null) {
    dbMonthObj = createDbMonthObj(date);
    dbDayObj = createDbDayObject(date);
    dbDayObj.hours[parseInt(hour)].exported = parseFloat(expVal);
    dbDayObj.hours[parseInt(hour)].imported = parseFloat(impVal);
    dbDayObj.exported = dbDayObj.hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.exported }, 0.0);
    dbDayObj.imported = dbDayObj.hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.imported }, 0.0);
    dbMonthObj.dailyPwrData[parseInt(day)-1]=(dbDayObj);
    await dbHandler('create', dbMonthObj)
    .then(function(acknowledged) {
      if (!acknowledged) { 
        console.log("ERROR: Failed to create new month object in DB!")
      } else {console.log("DB updated/created new month object.")}
    });
  } else {
    // Check if current dayObj exists in the received monthObj
    if (!dbMonthObj.dailyPwrData[parseInt(day)-1]) {
      dbDayObj = createDbDayObject(date);
      dbMonthObj.dailyPwrData[parseInt(day)-1] = dbDayObj;
    }
    if (isFullHour) {
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].isComplete = true; // Remove me
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].impExpIsComplete = true;
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].exported = parseFloat(expVal);
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].imported = parseFloat(impVal);
    } else {
      // Ackumulate hourly data
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].exported += parseFloat(expVal);
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].imported += parseFloat(impVal);
      if (dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].exported < 0 || dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].imported < 0) {
        // In case of bad luck in interrupted service or other reasons... clear the ackumulative values and
        // hope that the full hour update will fix correct values
        console.log("Negative values detected for hourly object, clearing ...");
        dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].exported = 0.0;
        dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].imported = 0.0;
      }
    }

    
    // Ackumulate daily data
    dbMonthObj.dailyPwrData[parseInt(day)-1].exported = dbMonthObj.dailyPwrData[parseInt(day)-1].hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.exported }, 0.0);
    dbMonthObj.dailyPwrData[parseInt(day)-1].imported = dbMonthObj.dailyPwrData[parseInt(day)-1].hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.imported }, 0.0);
    if (dbMonthObj.dailyPwrData[parseInt(day)-1].exported <0 || dbMonthObj.dailyPwrData[parseInt(day)-1].imported < 0) {
      // In case of bad luck in interrupted service or other reasons... clear the ackumulative values and
      // hope that the full hour update will fix correct valeus
      console.log("Negative values detected for daily object, clearing ...");
      dbMonthObj.dailyPwrData[parseInt(day)-1].exported = 0.0;
      dbMonthObj.dailyPwrData[parseInt(day)-1].imported = 0.0
    }

    // Ackumulate monthly data
    // Days may be missing in the mounthly array. Check for null entries in array!
    dbMonthObj.monthlyPwrData.exported =  dbMonthObj.dailyPwrData.reduce(function (acc, obj) { 
                                        let sold = 0.0;
                                        if (obj !== null && obj.hasOwnProperty('exported')) {sold=obj.exported}
                                        return parseFloat(acc) + sold;
                                      }, 0.0);
    dbMonthObj.monthlyPwrData.imported = dbMonthObj.dailyPwrData.reduce(function (acc, obj) {
                                        let bought = 0.0;
                                        if (obj !== null && obj.hasOwnProperty('imported')) {bought=obj.imported}
                                        return parseFloat(acc) + bought;
                                      }, 0.0);
    // Last update for the day ?
    if (parseInt(hour) === 23 && measurement.isStartOfHour) {
      let complete = true;
      for(let i=0; i<24; i++) {
        if (dbMonthObj.dailyPwrData[parseInt(day)-1].hours[i].isComplete === false) { //ToDo: replace with impExpIsComplete
          complete = false;
          break;
        }
      }
      dbMonthObj.dailyPwrData[parseInt(day)-1].impExpIsComplete = complete;
    }

    // Last update in month ?
    if (day == numDaysInMonth(year, month) && measurement.isStartOfHour) {
      const numDays = numDaysInMonth(year, month);
      let complete = true;
      for(let d=0; d <= numDays; d++) {
        if (dbMonthObj.dailyPwrData[parseInt(day)-1].impExpIsComplete === false) {
          complete = false;
          break;
        }
      }
      dbMonthObj.monthlyPwrData.impExpIsComplete = complete;
    }
    
    let setQuery = { identifier: { _id: dbMonthObj._id }, data: { $set: { dailyPwrData: dbMonthObj.dailyPwrData, monthlyPwrData: dbMonthObj.monthlyPwrData } } };
    await dbHandler('update', setQuery)
    .then((acknowledged) => {
        if (!acknowledged) {
            console.log("ERROR:   updateDb(), FAILED to update DB with PWR data");
        }else {console.log("DB updated with new measueremnt data.")}
    });
  }
}
