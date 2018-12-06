var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    assert = require('assert'),
    dotenv = require('dotenv');

async function main() {
  console.time("Query time");
  dotenv.load();
  let client;
  try {
    var url = "mongodb://" + process.env.TRANSAC_USR + ":" + process.env.TRANSAC_PWD + "@" + process.env.TRANSAC_SERVER + ":" + process.env.MONGO_DB_PORT + "?replicaSet=rs0";
    var options = { useNewUrlParser: true };

    client = await MongoClient.connect(url, options);
    const db = client.db("transactions");

    const read = await db.collection("newdexpocket")
      .find()
      .toArray();

    console.log(read.length);

    client.close();
    
    console.timeEnd("Query time");

  } catch (err) {
    console.log(err.stack);
  }
}

// Execute the main program
main();