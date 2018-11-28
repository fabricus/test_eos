var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    assert = require('assert'),
    dotenv = require('dotenv');

function process_results(result) {
  var nb = result.length;
  var unique_tsion = {};

  for (var i = 0; i < nb; i++) {
    // We group transactions by act_digest to avoid duplicate entries
    var hash = result[i].receipt.act_digest;
    unique_tsion[hash.toString()] = result[i].act.data;
    unique_tsion[hash.toString()].receiver = result[i].receipt.receiver;
    unique_tsion[hash.toString()].name = result[i].act.name;
    unique_tsion[hash.toString()].account = result[i].act.account;
  }

  // Index of unique Hashes / 1 hash per unique transaction (as 1 transaction can match multiple actions)
  var hashes = Object.keys(unique_tsion);
  hashes.forEach(function(y) {
    // For each transaction we insert the raw data into our local database
    try {
      //var json = process_memo(unique_tsion[y]);
      console.log(unique_tsion[y].receiver);

    } catch(error) {
      console.log(error);
    }
  });
}

(async function() {
  console.time("Query time");
  dotenv.load();
  let client;
  try {
    var url = "mongodb://" + process.env.MONGO_USR + ":" + process.env.MONGO_PWD + "@127.0.0.1:27017/";
    var options = { useNewUrlParser: true };
    client = await MongoClient.connect(url, options);
    const db = client.db("EOS");

    const read = await db.collection("action_traces")
              .find({ $or: [{"receipt.receiver": "newdexpocket"}, {"act.account": "newdexpocket"}] })
              .limit(10)
              .toArray();
      
    process_results(read);

    client.close();
  } catch (err) {
    console.log(err.stack);
  }
  console.timeEnd("Query time");   
})();  