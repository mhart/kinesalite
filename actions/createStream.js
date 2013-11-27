
module.exports = function createStream(store, data, cb) {

  var key = data.StreamName, metaDb = store.metaDb

  metaDb.lock(key, function(release) {
    cb = release(cb)

    metaDb.get(key, function(err) {
      if (err && err.name != 'NotFoundError') return cb(err)
      if (!err) {
        err = new Error
        err.statusCode = 400
        err.body = {
          __type: 'com.amazonaws.kinesis.v20130901#ResourceInUseException',
          message: '',
        }
        return cb(err)
      }

      var i, shards = new Array(data.ShardCount)
      for (i = 0; i < data.ShardCount; i++) {
        shards[i] = {
          AdjacentParentShardId: '',
          HashKeyRange: {
            EndingHashKey: '',
            StartingHashKey: '',
          },
          ParentShardId: '',
          SequenceNumberRange: {
            EndingSequenceNumber: '',
            StartingSequenceNumber: '',
          },
          ShardId: (i + 1).toString()
        }
      }
      data = {
        IsMoreDataAvailable: false,
        Shards: shards,
        StreamARN: 'arn:aws:kinesis:<region>:<number>:' + data.StreamName,
        StreamName: data.StreamName,
        StreamStatus: 'CREATING'
      }

      metaDb.put(key, data, function(err) {
        if (err) return cb(err)

        setTimeout(function() {

          // Shouldn't need to lock/fetch as nothing should have changed
          data.StreamStatus = 'ACTIVE'

          metaDb.put(key, data, function(err) {
            // TODO: Need to check this
            if (err) console.error(err)
          })

        }, store.createStreamMs)

        cb()
      })
    })
  })

}



