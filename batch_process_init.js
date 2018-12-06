var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    assert = require('assert'),
    dotenv = require('dotenv');

async function update_db(client, transactions, marketplace) {
    
  try {
    const db = client.db("transactions");
    const write = await db.collection(marketplace).insertOne(transactions);
  } catch (err) {
    console.log(transactions);
  }

  return 1;
}

async function process_results(result, drop) {
  var nb = result.length;
  var unique_tsion = {};
  var count = 0, unique = 0;

  for (var i = 0; i < nb; i++) {
    // We group transactions by act_digest to avoid duplicate entries
    let hash = result[i].receipt.act_digest;
    if (hash.toString() in unique_tsion) {
      unique++;
    }
    if (typeof result[i].act.data != "string") {
      unique_tsion[hash.toString()] = result[i].act.data;
      unique_tsion[hash.toString()].receiver = result[i].receipt.receiver;
      unique_tsion[hash.toString()].name = result[i].act.name;
      unique_tsion[hash.toString()].account = result[i].act.account;
    } else {
      count++;
      // raw hex data... this shouldn't happen so if we get here that means the server did something wrong when processing the transaction
    }
  }

  // We open the connection to the recording database
  var url = "mongodb://" + process.env.TRANSAC_USR + ":" + process.env.TRANSAC_PWD + "@" + process.env.TRANSAC_SERVER + ":" + process.env.MONGO_DB_PORT+ "?replicaSet=rs0";
  var options = { useNewUrlParser: true };
  let client;
  client = await MongoClient.connect(url, options);  

  if (drop) {
    try {
      // First we drop all the data to avoid duplicate entries
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

  console.log("excluded: " + count + " duplicate hash: " + unique);
  client.close();

  return 1;
}

async function main() {
  console.time("Total query time");

  var start_date = "2018-10-02T00:00:00.000Z";
  var cur_date = new Date(start_date);

  dotenv.load();
  let client;
  try {
    var url = "mongodb://" + process.env.READ_USR + ":" + process.env.READ_PWD + "@" + process.env.MONGO_DB_PLUGIN + ":" + process.env.MONGO_DB_PORT + "/EOS?replicaSet=rs0";
    var options = { useNewUrlParser: true };
    client = await MongoClient.connect(url, options);
    const db = client.db("EOS");

    console.time("Batch 1");
    const read = await db.collection("action_traces")
      .find(
        { $or:
            [
              {"receipt.receiver": "newdexpocket"},
              {"act.account": "newdexpocket"}
            ],
          "block_time" : {
            "$lt": cur_date.toISOString()
          }
        }
      )
      .toArray();

    var ok = await process_results(read, true);
    console.timeEnd("Batch 1");
    console.log("Batch 1: " + cur_date.toISOString() + " #" + read.length);

    var today = new Date(Date.now());
    var i = 2;

    while (cur_date < today) {
      let old_date = new Date(cur_date);
      cur_date.setDate(cur_date.getDate() + 1);
      console.time("Batch " + i);
      const read = await db.collection("action_traces")
        .find(
          { $or:
            [
              {"receipt.receiver": "newdexpocket"},
              {"act.account": "newdexpocket"}
            ],
            "block_time":
              {
                "$gte": old_date.toISOString(),
                "$lt": cur_date.toISOString()
              }
          }
        )
        .toArray();

      var ok = await process_results(read, false);
      console.timeEnd("Batch " + i);
      console.log("Batch " + i++ + ": " + cur_date + " #" + read.length);
    }

    client.close();
    console.timeEnd("Total query time");

  } catch (err) {
    console.log(err);
  }
}

// Execute the main program
main();