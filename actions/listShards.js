
module.exports = function listShards(store, data, cb) {

  //TODO get stream according to the given input [NextToken|StreamCreationTimestamp|StreamName]
  store.getStream(data.StreamName, function(err, stream) {
    if (err) return cb(err)

    cb(null, {Shards: stream.Shards})
  })
}
