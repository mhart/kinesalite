
module.exports = function getShardIterator(store, data, cb) {

  var metaDb = store.metaDb

  store.getStream(data.StreamName, false, function(err, stream) {
    if (err) {
      if (err.name == 'NotFoundError' && err.body) {
        err.body.message = 'Could not find shard ' + data.ShardId + ' in stream ' + data.StreamName +
          ' under account ' + metaDb.awsAccountId + '.'
      }
      return cb(err)
    }

    cb()
  })
}

