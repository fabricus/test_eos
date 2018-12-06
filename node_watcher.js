var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    assert = require('assert'),
    dotenv = require('dotenv');

function watch_insert(con, db, coll) {
  console.log(new Date() + ' watching: ' + coll);

  const insert_pipeline = [ { $match:
            { 
              operationType: 'insert',
              $or: [
                { "receipt.receiver": "newdexpocket" },
                { "act.account": "newdexpocket" }
              ]
            }
          }];

  con.db(db).collection(coll).watch(insert_pipeline)
    .on('change', data => {
      console.log(data)
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