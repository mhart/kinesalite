
module.exports = function getShardIterator(store, data, cb) {

  var metaDb = store.metaDb

  store.getStream(data.StreamName, false, function(err, stream) {
    if (err) {
      if (err.name == 'NotFoundError' && err.body) {
        err.body.message = 'Shard ' + data.ShardId + ' in stream ' + data.StreamName +
          ' under account ' + metaDb.awsAccountId + ' does not exist'
      }
      return cb(err)
    }

    cb()
  })
}

