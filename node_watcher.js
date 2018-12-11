var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    assert = require('assert'),
    dotenv = require('dotenv');


// XXX => Faire la même chose mais en batch processing
async function update_db(client, transactions, marketplace) {
    
  try {
    const db = client.db("transactions");
    const write = await db.collection(marketplace).insertOne(transactions);
  } catch (err) {
    console.log(transactions);
  }

  return 1;
}

async function process_results(result) {

  var trx = null;

  if (typeof result.fullDocument.act.data != "string") {
    trx = result.fullDocument.act.data;
    trx.receiver = result.fullDocument.receipt.receiver;
    trx.name = result.fullDocument.act.name;
    trx.account = result.fullDocument.act.account;
    trx.block_time = result.fullDocument.block_time;
  } else {
    // raw hex data... this shouldn't happen so if we get here that means the server did something wrong when processing the transaction
  }

  // We open the connection to the recording database
  var url = "mongodb://" + process.env.TRANSAC_USR + ":" + process.env.TRANSAC_PWD + "@" + process.env.TRANSAC_SERVER + ":" + process.env.MONGO_DB_PORT+ "?replicaSet=rs0";
  var options = { useNewUrlParser: true };
  let client;
  client = await MongoClient.connect(url, options);  

  try {
    // XXX - For now there is only one marketplace so it's hard coded
    res = await update_db(client, trx, "newdexpocket");
  } catch(error) {
    console.log(error);
  }

  client.close();

  return 1;
}

function watch_insert(con, db, coll) {
  console.log(new Date() + ' watching: ' + coll);

  const insert_pipeline = [ { $match:
            { 
              operationType: 'insert',
              $or: [
                { 'fullDocument.receipt.receiver': 'newdexpocket' },
                { 'fullDocument.act.account': 'newdexpocket' }
              ]
            }
          }];

  con.db(db).collection(coll).watch(insert_pipeline)
    .on('change', data => {
      // Write data into the distant DB
      process_results(data);
    });
}

async function run() {
  dotenv.load();
  var uri = "mongodb://" + process.env.READ_USR + ":" + process.env.READ_PWD + "@" + process.env.MONGO_DB_PLUGIN + ":" + process.env.MONGO_DB_PORT + "/EOS?replicaSet=rs0";

  try {
      con = await MongoClient.connect(uri, {"useNewUrlParser": true});
      watch_insert(con, 'EOS', 'action_traces');
    } catch (err) {
      console.log(err);
    }
}

// Execute the main program
run();