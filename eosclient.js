var Db = require('mongodb').Db,
    MongoClient = require('mongodb').MongoClient,
    assert = require('assert'),
    dotenv = require('dotenv');

dotenv.load();

var url = "mongodb://" + process.env.MONGO_USR + ":" + process.env.MONGO_PASSWORD + "@127.0.0.1:27017/";
var options = { useNewUrlParser: true };

function stringify(obj_from_json){
	var myEscapedJSONString = obj_from_json.replace(/ /g,'')
		.replace(/\\n/g, "\\n")
    	.replace(/\\'/g, "\\'")
        .replace(/\\"/g, '\\"')
        .replace(/\\&/g, "\\&")
        .replace(/\\r/g, "\\r")
        .replace(/\\t/g, "\\t")
        .replace(/\\b/g, "\\b")
        .replace(/\\f/g, "\\f")
        .replace(/:/g, "\":\"")
        .replace(/,/g, "\",\"")
        .replace(/{/g, "{\"")
        .replace(/}/g, "\"}");
    return myEscapedJSONString;
}

function process_memo(action) {
	var no_errors = true;
	try {
		var json = JSON.parse(action.memo);
		/* In the memo we have =>
			** type (buy-limit, sell-limit, cancel-order, trading-fees, delivery-transfer)
			** symbol (the pair being traded)
			** price (the quantity expressed in the currency used with symbol) => only if buy-limit or sell-limit
			** count (the quantity)
		*/
		return json;
	}
	catch(error) {
		// Probably because JSON isn't valid
		no_errors = false;
	}

	if (!no_errors) {
		try {
			var json = JSON.parse(stringify(action.memo));
			return json;
		} catch(error) {
			// Not JSON string
		}
	}

}

function process_results(result) {
  var nb = result.length;
  var unique_tsion = {};

  for (var i = 0; i < nb; i++) {
    // We group transactions by act_digest to avoid duplicate entries
    var hash = result[i].receipt.act_digest;
    unique_tsion[hash.toString()] = result[i].act.data;
  }

  var symbols = [];
  // Index of unique Hashes / 1 hash per unique transaction (as 1 transaction can match multiple actions)
  var hashes = Object.keys(unique_tsion);
  hashes.forEach(function(y) {
    // For each transaction we update the book matching the processed symbol
    try {
      var json = process_memo(unique_tsion[y]); // => Some format will make the process to bug, it's important to see what's matched and what's not in the real order book

      if (json.type == "buy-limit") {
        console.log("ordre " + json.type + " : " + json.count + ", " + json.symbol + " à " + json.price);
      }

      switch(json.type) {
        case "buy-limit":
        case "sell-limit":
        case "cancel-order":
        case "delivery-transfer":
        case "buy-market":
        case "sell-market":
        case "trading-fees":
        case "remaining-refund":
          break;
        default:
          // maybe the memo is something like => |random|:|839582|,|memo|:|cancel-order|,|type|:4
          console.log("non handled type: " + unique_tsion[y].memo);
      }

//        if (symbols.indexOf(json.symbol) == -1) {
//           symbols.push(json.symbol);
//        }
    } catch(error) {
//        // If we end-up here then the memo is not a JSON string
//        // Therefore we have to check on a case by case basis
//        var s = unique_tsion[y].memo;
//        if (s.indexOf("transfer fee") !== -1)
//        {
//          // we are on a token that charges transfer fees, the cost of the transfer is to be deducted from the book
//          // XXX - TBD
//          // Sample data => data: { from: 'newdexpocket', quantity: '2.239 IQ', memo: '0.1% transfer fee' }
//        }
    }
  });

//    // print all book from all symbols
//    symbols.forEach(function(sim) {
//      var res = dbo.collection(sim).find({}).toArray(function (err, items) {
//        //console.log(sim + " : ");
//        //console.log(items);
//      });
//    });

//    // XXX => To be deleted in prod env.
//    /*symbols.forEach(function(sim) {
//      dbo.collection(sim).drop();
//    });*/

//    db.close();
//  });
}

(async function() {
  console.time("Query time");
  let client;
  try {
    console.log("usr: " + process.env.MONGO_USR);
    client = await MongoClient.connect(url, options);
    const db = client.db("EOS");

    const read = await db.collection("action_traces")
              .find({ $or: [{"receipt.receiver": "newdexpocket"}, {"act.account": "newdexpocket"}] })
              .limit(100)
              .toArray();
      
      process_results(read);

      client.close();
    } catch (err) {
      console.log(err.stack);
    }
  console.timeEnd("Query time");   
})();  

// var pos = 100000, offset = 100;
// eos.getActions("newdexpocket").then(result => {
	
// 	var nb = result.actions.length;
// 	var sent = 0;
// 	var unique_tsion = {};

// 	for (var i = 0; i < nb; i++) {
// 		var tmp = tmp - 1 + i;
// 		// We group transactions by act_digest to avoid duplicate entries
// 		var hash = result.actions[i].action_trace.receipt.act_digest;
// 		unique_tsion[hash.toString()] = result.actions[i].action_trace.act.data;
// 	}

// 	// XXX => Add something to be aware of the blocks already processed - The goal is to avoid processing the same transactions twice

// 	MongoClient.connect(url, options, function(error, db) {
// 		var dbo = db.db("myinstance");
// 		var symbols = [];

// 		// Index of unique Hashes / 1 hash per unique transaction (usually 1 transaction = 3 actions)
// 		var hashes = Object.keys(unique_tsion);
// 		hashes.forEach(function(y) {
// 			// For each transaction we update the book matching the processed symbol
// 			try {
// 				var json = process_memo(unique_tsion[y]);

// 				//if (json.symbol == "EOX_EOS") console.log(json);

// 				if (json.type == "buy-limit") {
// 					console.log("ordre " + json.type + " : " + json.count + ", " + json.symbol + " à " + json.price);
// 				}

// 				switch(json.type) {
// 					case "buy-limit":				 				
// 						break;
// 					case "sell-limit":
// 						// First we look for a bid at the same price
// 						dbo.collection(json.symbol).find({price: json.price}).toArray(function(err, result) {
// 							// we only consider the first element as it should be unique
// 							if (result.length > 0) {
// 								var id = result[0]._id;
// 								var nb = result[0].qty;
// 								var count = json.count + nb;
							
							
// 							dbo.collection(json.symbol).updateOne(
// 	    						{ _id: id }, //id to find the element to be updated
// 								{
// 									$set: { qty: count },
// 									$currentDate: { lastModified: true }
// 								}
// 							);
// 								//console.log("on a fait un update : " + json.count + " nb : " + nb);

// 							}
// 	    				});

// 						var tObj = { _id: y, price: json.price, qty: json.count };
// 	    				dbo.collection(json.symbol).insertOne(tObj).catch(error => {
// 	    					// duplicate entry
// 	    				});

// 						break;
// 					case "cancel-order":
// 						break;
// 					case "delivery-transfer":
// 						// transfer that were delivered can be deducted from the the book
// 						break;
// 					case "buy-market":
// 					case "sell-market":
// 					case "trading-fees":
// 					case "remaining-refund":
// 						break;
// 					default:
// 						console.log("non handled type: " + json.type);
// 				}

// 				if (symbols.indexOf(json.symbol) == -1) {
// 	 			   symbols.push(json.symbol);
// 				}
// 			} catch(error) {
// 				// If we end-up here then the memo is not a JSON string
// 				// Therefore we have to check on a case by case basis
// 				var s = unique_tsion[y].memo;
// 				if (s.indexOf("transfer fee") !== -1)
// 				{
// 					// we are on a token that charges transfer fees, the cost of the transfer is to be deducted from the book
// 					// XXX - TBD
// 					// Sample data => data: { from: 'newdexpocket', quantity: '2.239 IQ', memo: '0.1% transfer fee' }
// 				}
// 			}
// 		});

// 		// print all book from all symbols
// 		symbols.forEach(function(sim) {
// 			var res = dbo.collection(sim).find({}).toArray(function (err, items) {
// 				//console.log(sim + " : ");
// 				//console.log(items);
// 			});
// 		});

// 		// XXX => To be deleted in prod env.
// 		/*symbols.forEach(function(sim) {
// 			dbo.collection(sim).drop();
// 		});*/

// 		db.close();
// 	});
// 	// Building the book for the XXX trading pair
// }).catch(error => {
// 	console.error(error)
// });