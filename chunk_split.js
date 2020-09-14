var splitChunks = function (namespace, shardname, sizeThreshold, test, est, verbosity) {

    /*
        TODO Improvements: Make it more generalizable and less tailored to their schema
    */

    db.getMongo().setReadPref("secondary");
    // Check if namespace is sharded
    var dbName = namespace.split(".")[0];
    var colName = namespace.split(".")[1];
    var collection = db.getSiblingDB(dbName).getCollection(colName);
    if (!collection.stats().sharded) {
        print(`Namespace  ${namespace} is not sharded!`);
        return;
    }

    var configDb = db.getSiblingDB("config");

    // Get shard key
    var keyDoc = configDb.collections.findOne({ "_id": namespace }, { "key": 1 });
    var key = keyDoc.key;
    var keyName = Object.keys(key)[0];

    // Get all the chunks for this namespace and shard
    var chunkCursor = configDb.chunks.find({ ns: namespace, shard: shardname }).sort({ _id: -1 }).noCursorTimeout();
    const BYTES_IN_MB = 1024 * 1024;

    function splitChunk(chunkDoc) {
        try {
            if (verbosity >= 2) {
                print(`Chunk doc is: ${JSON.stringify(chunkDoc)}`);
            }
            // Get size of chunk; set est=true to use an estimate of the chunk size, which will take the average document size
            // and multiply it by the number of documents in that chunk
            var dataSizeResult = configDb.runCommand({
                "datasize": namespace,
                "keyPattern": keyDoc.key,
                "min": chunkDoc.min,
                "max": chunkDoc.max,
                "estimate": est
            });
            var chunkSize = dataSizeResult.size;

            var chunkBounds = `[${JSON.stringify(chunkDoc.min)}, ${JSON.stringify(chunkDoc.max)}[`;
            print(`Analyzing chunk with bounds ${chunkBounds}`);
            print("-----------------------------------------------------------------");
            if (chunkDoc.jumbo) {
                print(`     Chunk with bounds ${chunkBounds} is a jumbo chunk`);
            }

            var queryDoc = {
                [keyName]: {
                    "$gte": chunkDoc.min[keyName],
                    "$lt": chunkDoc.max[keyName]
                }
            };
            var range = collection.distinct(keyName, queryDoc);
            var numShardKeyValues = range.length;

            if (verbosity >= 1) {
                print(`     Data size: ${JSON.stringify(dataSizeResult)}`);
                print(`     Submitting query ${JSON.stringify(queryDoc)} for key ${keyName}`);
                print(`     Found ${numShardKeyValues} distinct shard key values in range ${verbosity >= 2 ? `${JSON.stringify(range)}` : ''}`);
            }

            if (chunkSize < sizeThreshold) {
                print(`     Skipping chunk with bounds ${chunkBounds} as data size is only ${Math.round(chunkSize / BYTES_IN_MB)} MB`);
                return;
            }
            if (numShardKeyValues < 2) {
                print(`     Skipping chunk with bounds ${chunkBounds} as there is only a single value of the shard key in that chunk range`);
                return;
            }
            // If we would split the chunk and test=true, then proceed to simply print the potential split
            if (test) {
                print(`     TEST: Would split chunk with bounds ${chunkBounds} as data size is ${Math.round(chunkSize / BYTES_IN_MB)} MB`);
                /* Generate two fake splitted chunks */
                var results = collection.find(queryDoc).sort({ [keyName]: 1 }).toArray();
                var medianDocIndex = Math.floor(results.length / 2);
                var medianDoc = results[medianDocIndex];
                var medianValue = {
                    [keyName]: medianDoc[keyName]
                };
                var lowerFakeChunk = {
                    "ns": namespace,
                    "min": chunkDoc.min,
                    "max": medianValue,
                    "shard": shardname
                };
                var upperFakeChunk = {
                    "ns": namespace,
                    "min": medianValue,
                    "max": chunkDoc.max,
                    "shard": shardname
                };
                splitChunk(lowerFakeChunk);
                splitChunk(upperFakeChunk);
                return;
            }
            if (typeof(chunkDoc.min[keyName]) === "string") {
                print(`     Splitting chunk with bounds ${chunkBounds} at median point`);
                sh.splitFind(namespace, chunkDoc.min);

                var lowerChunk = configDb.chunks.findOne({ ns: namespace, min: chunkDoc.min }).noCursorTimeout();
                var upperChunk = configDb.chunks.findOne({ ns: namespace, max: chunkDoc.max }).noCursorTimeout();
                splitChunk(lowerChunk);
                splitChunk(upperChunk);
            }
        } catch (err) {
            print(`Encountered error when attempting to analyze chunk ${err}`);
        }
    }

    while (chunkCursor.hasNext()) {
        var chunkDoc = chunkCursor.next();
        splitChunk(chunkDoc);
    }
};
