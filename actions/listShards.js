
module.exports = function listShards(store, data, cb) {

  store.getStream(data.StreamName, function(err, stream) {
    if (err) return cb(err)

    cb(null, {Shards: stream.Shards})
  })
}