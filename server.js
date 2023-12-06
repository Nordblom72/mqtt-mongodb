#!/usr/local/bin node

const { dbHandler } = require('./src/db');
const mqtt = require('mqtt');

const numOfDecimals = 5;
let seconds;
let minutes;
let deviceRoot="NS1PWR/p1ib/";
let starting = true;

const monthsAsTextList = ['January', 'February', 'Mars', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const convertFromUtcToLocalDate = (utcDateObj) => {
  const offset = utcDateObj.getTimezoneOffset();
  return new Date(utcDateObj.getTime() - offset * 60000);
}

const numDaysInMonth = (year, month) => new Date(year, month, 0).getDate();
const updateFrequency = `${process.env.MONGODB_UPDATE_FREQUECY}`;
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
console.log("Database update period is set to every ", updatePeriod, " minute.");

const clientId = `mqtt_${Math.random().toString(16).slice(3)}`;
const conData = {
  clientId,
  clean: true,
  connectTimeout: 4000,
  reconnectPeriod: 1000,
  username: `${process.env.MQTT_USR}`,
  password: `${process.env.MQTT_PSW}`
}

//const client = mqtt.connect("mqtt://test.mosquitto.org");
//const client = mqtt.connect("mqtt://localhost:1883", conData);
const client = mqtt.connect("mqtt://192.168.1.215:1883", conData);

// The p1IB sends measurement data every 10 seconds
// DB will be updated as defined by updatePeriod (i.e. every updatePerion minutes)
// The measurement may start at any given time within the hour and data will be stored
// in DB as defined by updatePeriod.
let measurement = {
  date: null,
  numExpectedBursts: 6*updatePeriod,
  numReceivedBursts: 0,
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
  ongoing: false,
  isStartOfHour: false,
  isEndOfHour: false,
  gotFullMeasPeriod: false
}

const clearMeasObj = (measObj) => {
  measObj.date = null;
  measObj.numReceivedBursts = 0.0;
  measObj.startValueExport = 0.0;
  measObj.startValueImport = 0.0;

  if (measObj.isEndOfHour) {
    measObj.startOfHour.impValue = 0.0;
    measObj.startOfHour.expValue = 0.0;
    measObj.startOfHour.date = null;
    measObj.isEndOfHour = false;
  }
}

// ToDo: Improve value validation in case of bad respone from DB
const getMeasCacheinDb = async (date, hour) => {
  if (date.includes('T')) {
    date = date.split('T')[0];
  }
  return await dbHandler('get', { type: 'measCache' })
  .then(function(value) {
    if (value !== null) {
      if (value.date.split('T')[0] == date && parseInt(value.hour) == parseInt(hour)) {
        return [value.date, value.impVal, value.expVal];
      } else {
        return [null, null, null];
      }
    }
    return [null, null, null];
  });
}

const recoverStartData = async (date, aaa) => {
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

if (starting) {
  recoverStartData(convertFromUtcToLocalDate(new Date()).toISOString())
  .then(function(value) {
    starting = false;
    return value;
  });
}

const topics = {
  'NS1PWR/p1ib/p1ib_clock_date/state': {qos: 0},
  'NS1PWR/p1ib/p1ib_h_active_imp_q1_q4/state': {qos: 0},
  'NS1PWR/p1ib/p1ib_h_active_exp_q2_q3/state': {qos: 0}
};

client.on("connect", () => {
  console.log("MQTT Client Connected!")
  client.subscribe(topics, (err) => {
    if (err) {
      console.log(err)
    }
  });
});

client.on("message", (topic, message, packet) => {
  //let key=topic.replace(deviceRoot,'');
  if (!starting) {
    // Date + Time
    if (topic === 'NS1PWR/p1ib/p1ib_clock_date/state') {
      let date = convertFromUtcToLocalDate(new Date(message.toString())).toISOString(); // message is Buffer
      //console.log("       1: ", date);
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
        //measurement.date = date // Save the date for every start of hour
        measurement.ongoing = true;
        console.log("--------- New measurement period started: ", measurement.date);
      }

    // Import value  
    } else if (measurement.ongoing && topic === 'NS1PWR/p1ib/p1ib_h_active_imp_q1_q4/state') {
      //console.log("       2: ");
      // Last measurement burst for ongoing period?
      if (measurement.numReceivedBursts === (measurement.numExpectedBursts - 1) && (minutes%updatePeriod === 0) && (seconds < 8)) {
        measurement.sendToDbObj.startImpVal  = measurement.startValueImport; // Save startValueImport now as it will be overwtritten in next step
        measurement.sendToDbObj.stopImpVal   = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
        console.log("Got last meas for period: IMP");
      }
      // 1st meas burst of hour?
      if (minutes == 0  && (seconds < 8)) {
        measurement.startOfHour.prevImpValue = measurement.startOfHour.impValue; // Back-up before overwriting
        measurement.startOfHour.impValue = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
        console.log("  IMPORT (startOfHour.impValue): ", measurement.startOfHour.impValue);
      }
      // New Measuerement period start?
      if ((minutes%updatePeriod === 0) && (seconds < 8)) {
        measurement.startValueImport = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
        console.log("    IMPORT (startValueImport): ", measurement.startValueImport);
      }

    // Export value
    } else if (measurement.ongoing && topic === 'NS1PWR/p1ib/p1ib_h_active_exp_q2_q3/state') {
      //console.log("       3: ");
      // Last measurement burst for ongoing period?
      if (measurement.numReceivedBursts === (measurement.numExpectedBursts - 1) && (minutes%updatePeriod === 0) && (seconds < 8)) {
          measurement.sendToDbObj.startExpVal = measurement.startValueExport; // Save startValueExport now as it will be overwtritten in next step
          measurement.sendToDbObj.stopExpVal = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
          //console.log("Got last meas for period: EXP");
          measurement.gotFullMeasPeriod = true;
      }
      // 1st meas burst of hour?
      if (minutes == 0  && (seconds < 8)) {
        measurement.isStartOfHour = true;
        measurement.startOfHour.prevExpValue = measurement.startOfHour.expValue; //Back-up before overwiriting
        measurement.startOfHour.expValue = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
        //console.log("    EXPORT (.startOfHour.expValue): ", measurement.startOfHour.expValue);
      }
      // New Measuerement period start?
      if ((minutes%updatePeriod === 0) && (seconds < 8)) {
        measurement.startValueExport = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
        console.log("    EXPORT (startValueExport): ", measurement.startValueExport);
      } else {
        measurement.numReceivedBursts++;
        //console.log("       numReceivedBursts ++ ", measurement.numReceivedBursts);
      }
    }

    if (measurement.gotFullMeasPeriod) {
      console.log("*** gotFullMeasPeriod ***")
      measurement.gotFullMeasPeriod = false;
      measurement.numReceivedBursts = 0;
      let tmpObj = {
        date:             measurement.date,
        startValueImport: measurement.sendToDbObj.startImpVal,
        startValueExport: measurement.sendToDbObj.startExpVal,
        stopValueImport:  measurement.sendToDbObj.stopImpVal,
        stopValueExport:  measurement.sendToDbObj.stopExpVal,
        startOfHour: {
          prevImpValue: measurement.startOfHour.prevImpValue,
          prevExpValue: measurement.startOfHour.prevExpValue,
          impValue:     measurement.startOfHour.impValue,
          expValue:     measurement.startOfHour.expValue,
          prevDate:     measurement.startOfHour.prevDate,
          date:         measurement.startOfHour.date
        },
        isStartOfHour: measurement.isStartOfHour,
        isEndOfHour: measurement.isEndOfHour
      };

      let send = true;
      if (tmpObj.startValueImport == 0 || tmpObj.startValueExport == 0) {
        console.log(" BAD VALUES");
        send = false;
      }
      if (send) {
        updateDb(JSON.parse(JSON.stringify(tmpObj))); // Send a copy of the measurement Object to updateDb ...
      }

      // Clear
      measurement.sendToDbObj.date = null;
      measurement.sendToDbObj.startImpVal = 0.0;
      measurement.sendToDbObj.startExpVal = 0.0;
      measurement.sendToDbObj.stopImpVal = 0.0;
      measurement.sendToDbObj.stopExpVal = 0.0;
      measurement.isStartOfHour = false;
    }

    if (measurement.numReceivedBursts >= measurement.numExpectedBursts) {
      // Ooops, something went wrong, reset the counter
      console.log("WARNING, bad state: numReceivedBursts counter exceeds max value. Clearing the coutner!")
      measurement.numReceivedBursts = 0;
    }
  }
  //client.end(); 
});


const createDbDayObject = (dateStr) => {
  hrObj = {
    produced: 0.0,
    sold: 0.0,
    bought: 0.0,
    temperature: null,
    isComplete: false,
    impExpIsComplete: false
  }
  dayObj = {
    date: dateStr.split('T')[0],
    produced: 0.0,
    sold: 0.0,
    bought: 0.0,
    impExpIsComplete: false,
    hours: []
  }
  for(let i=0; i<24; i++) {
    dayObj.hours[i] =  Object.assign({}, hrObj);
  }
  return dayObj;
}

const createDbMonthObj = (dateStr) => {
  [year, month, ...rest] = dateStr.split('T')[0].split('-');

  dbMonthObj = {
    year: parseInt(year),
    monthName: monthsAsTextList[month-1], // monthNr  0-11
    monthlyPwrData: {
      produced: 0.0,
      sold: 0.0,
      bought: 0.0,
      impExpIsComplete: false
    },
    dailyPwrData: []
  }
  // ToDo: hmmm, create empty days until yesteday or ignore?
  /*const dayObj = createDbDayObject('');
  for(let i=0; i<numDaysInMonth; i++) {
    dbMonthObj.dailyPwrData[i] = Object.assign({}, dayObj);
  }*/
  return dbMonthObj;
}

const updateMeasCacheinDb = async (date, hour, impVal, expVal) => {
  //const newCacheData = { type: 'measCache', date: date.split('T')[0], hour: hour, impVal: impVal, expVal: expVal };
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
      }
      console.log("  updateMeasCacheinDb(), DB updated with hourly start-up data.");
    });
  } else {
    await dbHandler('create', newCacheData)
    .then(function(value) {
    })
  }
}

const updateDb = async (measurement) => {
  //console.log(JSON.stringify(measurement, null, 2));
  let hour, minute, rest;
  let year, month, day;
  let impVal, expVal = 0.0;
  let date;
  let isFullHour = false;

  console.log("");
  if (measurement.startOfHour.prevDate !== null && measurement.isStartOfHour) {
    console.log(JSON.stringify(measurement, null, 2));
    date = measurement.startOfHour.prevDate; // It s start of hour, but the meas data is for last meas period of previous hour
  } else {
    date = measurement.date; // Ackumulated update
  }

  // Save start of hour data in DB as backup ?
  if (measurement.isStartOfHour) {
    console.log("Is start of hour: ",measurement.startOfHour.date);
    [hr, ...rest] = measurement.startOfHour.date.split('T')[1].split(':');
    await updateMeasCacheinDb(measurement.startOfHour.date, parseInt(hr), parseFloat(measurement.startOfHour.impValue), parseFloat(measurement.startOfHour.expValue)); 
  }

  [hour, minute, ...rest] = date.split('T')[1].split(':');
  [year, month, day, ...rest] = date.split('T')[0].split('-');

  // Calculate diff values for impVal & expVal
  if (measurement.startOfHour.prevDate !== null && measurement.isStartOfHour) {
      console.log("Is full hour: ",measurement.startOfHour.date );
      isFullHour = true;
      impVal = parseFloat(measurement.startOfHour.impValue - measurement.startOfHour.prevImpValue).toFixed(numOfDecimals);
      expVal = parseFloat(measurement.startOfHour.expValue - measurement.startOfHour.prevExpValue).toFixed(numOfDecimals);
      console.log("FULL Hr IMP: ", impVal);
      console.log("FULL Hr EXP: ", expVal);
      //[hour, minute, ...rest] = measurement.startOfHour.date.split('T')[1].split(':');
      //[year, month, day, ...rest] = measurement.startOfHour.date.split('T')[0].split('-');
  } else {
    console.log("Is ackumulated update: ");
    //console.log("ACK IMP: ", measurement.stopValueImport - measurement.startValueImport);
    //console.log("ACK EXP: ", measurement.stopValueExport - measurement.startValueExport);
    impVal = parseFloat(measurement.stopValueImport - measurement.startValueImport).toFixed(numOfDecimals);
    expVal = parseFloat(measurement.stopValueExport - measurement.startValueExport).toFixed(numOfDecimals);
    //console.log("    ACK IMP: ", impVal);
    //console.log("    ACK EXP: ", expVal);
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
    dbDayObj.hours[parseInt(hour)].sold = parseFloat(expVal);
    dbDayObj.hours[parseInt(hour)].bought = parseFloat(impVal);
    dbDayObj.sold = dbDayObj.hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.sold }, 0.0);
    dbDayObj.bought = dbDayObj.hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.bought }, 0.0);
    dbMonthObj.dailyPwrData[parseInt(day)-1]=(dbDayObj);
    await dbHandler('create', dbMonthObj)
    .then(function(value) {
    })
  } else {
    // Check if current dayObj exists in the received monthObj
    if (!dbMonthObj.dailyPwrData[parseInt(day)-1]) {
      dbDayObj = createDbDayObject(date);
      dbMonthObj.dailyPwrData[parseInt(day)-1] = dbDayObj;
    }
    if (isFullHour) {
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].isComplete = true; // Remove me
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].impExpIsComplete = true;
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].sold = parseFloat(expVal);
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].bought = parseFloat(impVal);
    } else {
      console.log("Ackumulating hourly data");
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].sold += parseFloat(expVal);
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].bought += parseFloat(impVal);
      if (dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].sold < 0 || dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].bought < 0) {
        // In case of bad luck in interrupted service or other reasons... clear the ackumulative values and
        // hope that the full hour update will fix correct values
        console.log("Negative values detected for hourly object, clearing ...");
        dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].sold = 0.0;
        dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].bought = 0.0;
      }
    }

    console.log("Ackumulating daily data");
    dbMonthObj.dailyPwrData[parseInt(day)-1].sold = dbMonthObj.dailyPwrData[parseInt(day)-1].hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.sold }, 0.0);
    dbMonthObj.dailyPwrData[parseInt(day)-1].bought = dbMonthObj.dailyPwrData[parseInt(day)-1].hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.bought }, 0.0);
    if (dbMonthObj.dailyPwrData[parseInt(day)-1].sold <0 || dbMonthObj.dailyPwrData[parseInt(day)-1].bought < 0) {
      // In case of bad luck in interrupted service or other reasons... clear the ackumulative values and
      // hope that the full hour update will fix correct valeus
      console.log("Negative values detected for daily object, clearing ...");
      dbMonthObj.dailyPwrData[parseInt(day)-1].sold = 0.0;
      dbMonthObj.dailyPwrData[parseInt(day)-1].bought = 0.0
    }

    console.log("Ackumulating monthly data"); // Days may be missing in the mounthly array. Check for null entries in array!
    dbMonthObj.monthlyPwrData.sold =  dbMonthObj.dailyPwrData.reduce(function (acc, obj) { 
                                        let sold = 0.0;
                                        if (obj !== null && obj.hasOwnProperty('sold')) {sold=obj.sold}
                                        return parseFloat(acc) + sold 
                                      }, 0.0);
    dbMonthObj.monthlyPwrData.bought = dbMonthObj.dailyPwrData.reduce(function (acc, obj) {
                                        let bought = 0.0;
                                        if (obj !== null && obj.hasOwnProperty('bought')) {bought=obj.bought}
                                        return parseFloat(acc) + bought 
                                      }, 0.0);
    //console.log("MB ", dbMonthObj.monthlyPwrData.bought);
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
           return "FAILED to update DB with PWR data";
        }
        console.log("  updateDb(), DB updated with insertion of PWR data");
          return ("DB updated with insertion of PWR data");
    });
  }

  //console.log("QUERY: ", query);
  //console.log("BOUGHT ", impVal);
  //console.log("SOLD   ", expVal);
  //console.log(JSON.stringify(dbMonthObj, null, 2));

	/*let query = { 
    $push: { events: { event: {  value:message, when:new Date() } } }, 
    upsert:true
  };*/
}
