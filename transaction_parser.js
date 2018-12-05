var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    assert = require('assert'),
    dotenv = require('dotenv');

/*async function process_results(result) {
  var nb = result.length;
  var unique_tsion = {};

  for (var i = 0; i < nb; i++) {
    // We group transactions by act_digest to avoid duplicate entries
    let hash = result[i].receipt.act_digest;
    if (typeof result[i].act.data != "string") {
      unique_tsion[hash.toString()] = result[i].act.data;
      unique_tsion[hash.toString()].receiver = result[i].receipt.receiver;
      unique_tsion[hash.toString()].name = result[i].act.name;
      unique_tsion[hash.toString()].account = result[i].act.account;
    } else {
      // raw hex data... this shouldn't happen so if we get here that means the server did something wrong when processing the transaction
    }
  }

  // We open the connection to the recording database
  var url = "mongodb://" + process.env.TRANSAC_USR + ":" + process.env.TRANSAC_PWD + "@" + process.env.TRANSAC_SERVER + ":" + process.env.MONGO_DB_PORT;
  var options = { useNewUrlParser: true };
  let client;
    
  try {
    // First we drop all the data to avoid duplicate calls
    client = await MongoClient.connect(url, options);

    const db = client.db("transactions");
    var collections = await db.command( { listCollections: 1 } );
    var nb_coll = Object.keys(collections.cursor.firstBatch).length;

    for (var i = 0; i < nb_coll; i++) {
      var item = collections.cursor.firstBatch[i];
      const drop = await db.collection(item.name).drop();
    }

  } catch (error) {
    console.log(error);
  }
  // Index of unique Hashes / 1 hash per unique transaction (as 1 transaction can match multiple actions)
  var hashes = Object.keys(unique_tsion);
  for (var i = 0; i < hashes.length; i++) {
    // For each transaction we insert the raw data into our local database
    try {
      // XXX - For now there is only one marketplace so it's hard coded
      res = await update_db(client, unique_tsion[hashes[i]], "newdexpocket");

    } catch(error) {
      console.log(error);
    }
  };

  client.close();

  return 1;
}*/

/*function watch_insert(con, db, coll) {
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

async function run(uri) {
  try {
      con = await MongoClient.connect(uri, {"useNewUrlParser": true});
      watch_insert(con, 'EOS', 'action_traces');
    } catch (err) {
      console.log(err);
    }
}*/

async function main() {
  console.time("Query time");
  dotenv.load();
  let client;
  try {
    var url = "mongodb://" + process.env.READ_USR + ":" + process.env.READ_PWD + "@" + process.env.MONGO_DB_PLUGIN + ":" + process.env.MONGO_DB_PORT + "/EOS?replicaSet=rs0";
    var options = { useNewUrlParser: true };
    client = await MongoClient.connect(url, options);
    const db = client.db("transactions");

    const read = await db.collection("action_traces")
      .find(
        { $or:
          [
            {"receipt.receiver": "newdexpocket"},
            {"act.account": "newdexpocket"}
          ]
        })
      .limit(10)
      .toArray();

    console.log(read);

    client.close();
    
    console.timeEnd("Query time");
    // Then we plugin a Mongo ChangeStream to forward any new incoming transaction
    //run(url);

  } catch (err) {
    console.log(err.stack);
  }
}

// Execute the main program
main();