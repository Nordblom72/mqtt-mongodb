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

const setUpdatePeriodMinutes = () => {
  const validNumbers = [2,3,4,5,6,10,12,15,20,30]; // Number of DB updates per hour
  if (`${process.env.MONGODB_UPDATE_FREQUECY}` != 'undefined' && typeof(parseInt(`${process.env.MONGODB_UPDATE_FREQUECY}`)) === 'number' ) {
    if (validNumbers.includes(parseInt(`${process.env.MONGODB_UPDATE_FREQUECY}`))) {
      console.log("Desired update frequency towards Mongo DB is ", parseInt(`${process.env.MONGODB_UPDATE_FREQUECY}`));
      return (60/ parseInt(`${process.env.MONGODB_UPDATE_FREQUECY}`));
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
  periodLength: 10*updatePeriod, //Seconds
  numExpectedBursts: 6*updatePeriod,
  numReceivedBursts: 0,
  startValueImport: 0.0,
  stopValueImport: 0.0,
  startValueExport: 0.0,
  stopValueExport: 0.0,
  startOfHour: {
    impValue: 0.0,
    expValue: 0.0,
    date: null
  },
  isEndOfHour: false,
}

const clearMeasObj = (measObj) => {
  measObj.date = null;
  measObj.numReceivedBursts = 0.0;
  measObj.startValueExport = 0.0;
  measObj.startValueImport = 0.0;
  measObj.stopValueExport = 0.0;
  measObj.stopValueImport = 0.0;

  if (measObj.isEndOfHour) {
    measObj.startOfHour.impValue = 0.0;
    measObj.startOfHour.expValue = 0.0;
    measObj.startOfHour.date = null;
    measObj.isEndOfHour = false;
  }
}

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
  recoverStartData(convertFromUtcToLocalDate(new Date()).toISOString());
  starting = false;
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
  if (topic === 'NS1PWR/p1ib/p1ib_clock_date/state') {
    minutes = message.toString().split(':')[1];
    seconds =  message.toString().split(':')[2].split('Z')[0];
    if ((minutes%updatePeriod === 0) && (seconds < 8)) {
      clearMeasObj(measurement);
      measurement.date = convertFromUtcToLocalDate(new Date(message.toString())).toISOString(); // message is Buffer
      console.log("New measurement period started: ", measurement.date);
    }
    if (minutes == 0  && (seconds < 8)) {
      measurement.startOfHour.date = convertFromUtcToLocalDate(new Date(message.toString())).toISOString(); // message is Buffer
      measurement.date = null; 
    }
  } else if (topic === 'NS1PWR/p1ib/p1ib_h_active_imp_q1_q4/state') {
    // Start of measurement period ?
    if ((minutes%updatePeriod === 0) && (seconds < 8)) {
      measurement.numReceivedBursts = 1;
      measurement.startValueImport = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
    } else if ((minutes%updatePeriod === 1) && (seconds > 48)) { // End of measurement period ?
      measurement.stopValueImport = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
    }

    if (minutes == 0  && (seconds < 8)) {
      measurement.startOfHour.impValue = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
    }
  } else if (topic === 'NS1PWR/p1ib/p1ib_h_active_exp_q2_q3/state') {
    // Start of measurement period ?
    if ((minutes%updatePeriod === 0) && (seconds < 8)) {
      measurement.startValueExport = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
    } else if ((minutes%updatePeriod === 1) && (seconds > 48)) {  // End of measurement period ?
      measurement.stopValueExport = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
      measurement.numReceivedBursts++;
      if (minutes > (60-updatePeriod)) {
        measurement.isEndOfHour = true;
      }
    } else {
      measurement.numReceivedBursts++;
    }
    if (minutes == 0  && (seconds < 8)) {
      measurement.startOfHour.expValue = parseFloat(message.toString()).toFixed(numOfDecimals); // message is Buffer
    }
  }

  if (measurement.numReceivedBursts === measurement.numExpectedBursts) {
    console.log("Diff IMP: ", measurement.stopValueImport-measurement.startValueImport);
    console.log("Diff EXP: ", measurement.stopValueExport-measurement.startValueExport);
    console.log("Send to DB")
    updateDb(JSON.parse(JSON.stringify(measurement))); // Send a copy of the measurement Object to updateDb ...

    clearMeasObj(measurement);
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
  let hour, minute, rest;
  let year, month, day;
  let impVal, expVal = 0.0;
  let date;
  let islastUpdateForDay, isFullHour = false;

  console.log("")
  if (measurement.startOfHour.date !== null  && measurement.date === null) {
    console.log("Is start of hour: ",measurement.startOfHour.date );
    date = measurement.startOfHour.date;
    [hr, ...rest] = measurement.startOfHour.date.split('T')[1].split(':');
    await updateMeasCacheinDb(date, parseInt(hr), parseFloat(measurement.startOfHour.impValue), parseFloat(measurement.startOfHour.expValue));
  } else {
    console.log("Is NOT start of hour");
    date = measurement.date;
  }

  [hour, minute, ...rest] = date.split('T')[1].split(':');
  [year, month, day, ...rest] = date.split('T')[0].split('-');

  // Calculate diff values for impVal & expVal
  if (measurement.startOfHour.date !== null && measurement.isEndOfHour) {
      console.log("Is full hour: ",measurement.startOfHour.date );
      isFullHour = true;
      impVal = parseFloat(measurement.stopValueImport - measurement.startOfHour.impValue).toFixed(numOfDecimals);
      expVal = parseFloat(measurement.stopValueExport - measurement.startOfHour.expValue).toFixed(numOfDecimals);
  } else {
    console.log("Is ackumulated update: ");
    console.log("DDD IMP: ", measurement.stopValueImport - measurement.startValueImport);
    console.log("DDD EXP: ", measurement.stopValueExport - measurement.startValueExport);
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
        // hope that the full hour update will fix correct valeus
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
    console.log("MB ", dbMonthObj.monthlyPwrData.bought);
    // Last update for the day ?
    if (parseInt(hour) === 23 && measurement.isEndOfHour) {
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
    if (day == numDaysInMonth(year, month) && measurement.isEndOfHour) {
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

  console.log("QUERY: ", query);
  console.log("BOUGHT ", impVal);
  console.log("SOLD   ", expVal);
  //console.log(JSON.stringify(dbMonthObj, null, 2));

	/*let query = { 
    $push: { events: { event: {  value:message, when:new Date() } } }, 
    upsert:true
  };*/
}
