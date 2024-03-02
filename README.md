# mqtt-mongodb
[Link to project's main page](https://github.com/Nordblom72/SolarAndPwrOverview)<br/>

This MQTT client listens for messages sent from a P1IB power meter and saves the data in a MongoDb database.<br/>

The P1IB device is jacked into the RJ12 outlet of the power meter. The RJ12 outlet is normally disabled and needs to be enabled. Check with your power-grid supplier how to do that. In this case the vendor is Eon and the outlet can be enabled/disabled in the Eon's app.<br/>

P1IB sends bursts of various measurement data every 10 seconds on the MQTT bus. This service only needs to listen for the import and export values, thus only p1ib_h_active_exp_q2_q3, p1ib_h_active_imp_q1_q4 and p1ib_clock_date are registered as subscription topics towards the MQTT broker.

The received power values are ackumulated and stored to the database with the granuality of one hour. The frequency of database update (also affecting the length of a measurement period) is configurable via an environment variable. Default is every 5:th minute. <br/>

There is also an data object holding the import and export yield values which also are stored in the database. This object is saved on every start of hour. The data object serves as a simple recovery solution for cases where there might be a restart of the service or a power/wifi/internet outage and the fault gets restored within the same hour.

# Configuration variables
| Var name| Value | Comment
| --------------|-----------|--------------|
| MONGODB_UPDATE_FREQUECY | Valid values: 2,3,4,5,6,10,12,15,20,30 | Default is 12
| MQTT_USR |MQTT user|
| MQTT_PSW | MQTT user-password |
| MONGODB_USER | MongoDb user |
| MONGODB_PSW | MongoDb user-password |
| MONGODB_URI | MongoDb URL |
| MONGODB_DATABASE | MongoDb database name |
| MONGODB_COLLECTION | MongoDb database collection |

# References
* [Link to P1IB github repo](https://github.com/remne/p1ib)
* [Link to Remnetech, the offical site of P1IB](https://remne.tech/)<br/>
* [MQTT official site](https://mqtt.org/)
* [MQTT Mosquitto broker ](https://mosquitto.org/)

# ToDo
* Increase DbMongo timeout
* Implement proper error handling for database failures
* Implement some kind of critical logging to be stored in MongoDb