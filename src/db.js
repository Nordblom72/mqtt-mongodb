const { MongoClient, ServerApiVersion  } = require('mongodb')

const username = "";
const password = "";
const atlasPath = "";
const database = "solardb";
const MONGODB_DEFAULTS = {
  url: 'mongodb+srv://' + `${username}` + ':' + `${password}` + `${atlasPath}`,
  database: database
}

const mongoClient = new MongoClient(MONGODB_DEFAULTS.url, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  }
});

const dbHandler = async (opType, context) => {
  console.log("    dbHandler(), opType: ", opType);
  let rsp;
  try {
    const clientPromise = await mongoClient.connect({ maxIdleTimeMS : 270000, minPoolSize : 2, maxPoolSize : 4 });
    // Send a ping to confirm a successful connection
    await mongoClient.db("admin").command({ ping: 1 });
    console.log("    dbHandler(), Pinged your deployment. You successfully connected to MongoDB!");

    const database = clientPromise.db(MONGODB_DEFAULTS.database);
    const collection = database.collection('monthlyPwrData');
    
    if (opType === 'update') {
      rsp = await collection.updateOne(context.identifier, context.data);
      rsp = rsp.acknowledged;
    } else if (opType === 'create') { 
      rsp = await collection.insertOne(context);
    } else { //get
      rsp = await collection.findOne(context);
    }
  } catch (error) {
    console.log("ERROR:  dbHandler(), fetching data from DB!");
    console.log(error.toString());
  } finally {
    // Ensures that the client will close when you finish/error
    await mongoClient.close();
    console.log("    dbHandler(), Successfully dis-connected from MongoDB!");
    return (rsp);
  } 
}


module.exports = { dbHandler }
