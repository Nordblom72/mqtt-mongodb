const { dbHandler } = require('./src/db');
const mqtt = require('mqtt');

let deviceRoot="NS1PWR/p1ib/";
const updatePeriod = 2; // minutes. Must be > 1
const numOfDecimals = 5;
let seconds;
let minutes;

// The p1IB sends measurement data every 10 seconds
// DB will be updated as defined by updatePeriod (i.e. every updatePerion minutes)
// The measurement may start at any given time within the hour and data will be stored
// in DB as defined by updatePeriod.
// If the data by the end of hour does not hold a complete hour, the value in DB will be cleared.
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

//let updateSuccesful = false;
const monthsAsTextList = ['January', 'February', 'Mars', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

const clientId = `mqtt_${Math.random().toString(16).slice(3)}`;
const conData = {
  clientId,
  clean: true,
  connectTimeout: 4000,
  reconnectPeriod: 1000,
  username: '',
  password: ''
}

const convertFromUtcToLocalDate = (utcDateObj) => {
  const offset = utcDateObj.getTimezoneOffset();
  return new Date(utcDateObj.getTime() - offset * 60000);
}

console.log("Server started: ", convertFromUtcToLocalDate(new Date()));

//const client = mqtt.connect("mqtt://test.mosquitto.org");
const client = mqtt.connect("mqtt://192.168.1.215:1883", conData);

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
      console.log("New measurement period started: ", measurement.date)
    }
    if (minutes == 0  && (seconds < 8)) {
      measurement.startOfHour.date = convertFromUtcToLocalDate(new Date(message.toString())).toISOString(); // message is Buffer
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
      if (minutes == 59) {
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
    updateDb(Object.assign(measurement));

    clearMeasObj(measurement);
  }
  //client.end(); 
});

const createDbDayObject = (dateStr) => {
  hrObj = {
    produced: 0.0,
    sold: 0.0,
    bought: 0.0,
    temperature: null
  }
  dayObj = {
    date: dateStr.split('T')[0],
    produced: 0.0,
    sold: 0.0,
    bought: 0.0,
    isComplete: false,
    hours: []
  }
  for(let i=0; i<24; i++) {
    dayObj.hours[i] =  Object.assign({}, hrObj);
  }
  return dayObj;
}

const createDbMonthObj = (dateStr) => {
  [year, month, ...rest] = dateStr.split('T')[0].split('-');
  //const numDaysInMonth = convertFromUtcToLocalDate(new Date()).getDate(); //Up to current day in month
  //console.log("NUM DAYS in month ", numDaysInMonth);

  dbMonthObj = {
    year: parseInt(year),
    monthName: monthsAsTextList[month-1], // monthNr  0-11
    monthlyPwrData: {
      produced: 0.0,
      sold: 0.0,
      bought: 0.0
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

const updateDb = async (measurement) => {
  let hour, minute, rest;
  let year, month, day;
  let impVal, expVal = 0.0;
  let date;
  let isNewDbMonthObj, isNewDbDayObj, isFullHour = false;

  if (measurement.startOfHour.date !== null) {
    console.log("Is start of hour: ",measurement.startOfHour.date );
    date = measurement.startOfHour.date;
  } else {
    console.log("Is NOT start of hour");
    date = measurement.date;
  }
  [hour, minute, ...rest] = measurement.date.split('T')[1].split(':');
  [year, month, day, ...rest] = measurement.date.split('T')[0].split('-');

  if (measurement.startOfHour.date !== null && measurement.isEndOfHour) {
    console.log("Is full hour: ",measurement.startOfHour.date );
    console.log(measurement.stopValueImport);
    isFullHour = true;
    impVal = parseFloat(measurement.stopValueImport - measurement.startOfHour.impValue).toFixed(numOfDecimals);
    expVal = parseFloat(measurement.stopValueExport - measurement.startOfHour.expValue).toFixed(numOfDecimals);
  } else {
    console.log("Is ackumulated update: ")
    console.log("DDD IMP: ", measurement.stopValueImport - measurement.startValueImport)
    impVal = parseFloat(measurement.stopValueImport - measurement.startValueImport).toFixed(numOfDecimals);
    expVal = parseFloat(measurement.stopValueExport - measurement.startValueExport).toFixed(numOfDecimals);
  }

  let monthName = monthsAsTextList[month-1]; // monthNr  0-11
  let query = { year: parseInt(year), monthName: monthName };
  let dbMonthObj = await dbHandler('get', query)
              .then(function(value) {
                 //console.log("DANNE GET....", value);
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
      console.log("DANNE create....", value);
  })
  } else {
    //console.log(JSON.stringify(dbMonthObj, null, 2));
    // Check if current dayObj exists in the received monthObj
    if (!dbMonthObj.dailyPwrData[parseInt(day)-1]) {
      console.log("DayObj in DB is missing...creating one")
      dbDayObj = createDbDayObject(date);
      dbMonthObj.dailyPwrData[parseInt(day)-1] = dbDayObj;
    }
    if (isFullHour) {
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].sold = parseFloat(expVal);
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].bought = parseFloat(impVal);
    } else {
      console.log("Ackumulating hour");
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].sold += parseFloat(expVal);
      dbMonthObj.dailyPwrData[parseInt(day)-1].hours[parseInt(hour)].bought += parseFloat(impVal);
    }
    console.log("Ackumulating daily");
    dbMonthObj.dailyPwrData[parseInt(day)-1].sold = dbMonthObj.dailyPwrData[parseInt(day)-1].hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.sold }, 0.0);
    dbMonthObj.dailyPwrData[parseInt(day)-1].bought = dbMonthObj.dailyPwrData[parseInt(day)-1].hours.reduce(function (acc, obj) { return parseFloat(acc) + obj.bought }, 0.0);
    let setQuery = { identifier: { _id: dbMonthObj._id }, data: { $set: { dailyPwrData: dbMonthObj.dailyPwrData } } };
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
