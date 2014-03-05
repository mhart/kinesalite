var Big = require('big.js')

var FULL_HASH_RANGE = '340282366920938463463374607431768211456'

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
          __type: 'ResourceInUseException',
          message: '',
        }
        return cb(err)
      }

      if (data.ShardCount > 10) {
        err = new Error
        err.statusCode = 400
        err.body = {
          __type: 'LimitExceededException',
          message: 'This request would exceed the shard limit for the account ' + metaDb.awsAccountId + ' in us-east-1. ' +
            'Current shard count for the account: 0. Limit: 10. Number of additional shards that would have ' +
            'resulted from this request: ' + data.ShardCount + '. Shard limit increases can be requested by ' +
            'submitting a case to the AWS Support Center at https://aws.amazon.com/support/createCase?' +
            'type=service_limit_increase&serviceLimitIncreaseType=kinesis-limits.',
        }
        return cb(err)
      }

      Big.RM = 0 // round down

      var i, shards = new Array(data.ShardCount), shardHash = Big(FULL_HASH_RANGE).div(data.ShardCount)
      for (i = 0; i < data.ShardCount; i++) {
        shards[i] = {
          //ParentShardId: '',
          //AdjacentParentShardId: '',
          HashKeyRange: {
            StartingHashKey: shardHash.times(i).toFixed(0),
            EndingHashKey: shardHash.times(i + 1).minus(1).toFixed(0),
          },
          SequenceNumberRange: {
            StartingSequenceNumber: '49537279973004700513262647557344055618854783026326405121',
            //EndingSequenceNumber: '49537279973004700513262647557344055618854783026326405121',
          },
          ShardId: 'shardId-00000000000' + i // TODO: Fix this for > 10
        }
      }
      data = {
        HasMoreShards: false,
        Shards: [],
        StreamARN: 'arn:aws:kinesis:us-east-1:' + metaDb.awsAccountId + ':stream/' + data.StreamName,
        StreamName: data.StreamName,
        StreamStatus: 'CREATING'
      }

      metaDb.put(key, data, function(err) {
        if (err) return cb(err)

        setTimeout(function() {

          // Shouldn't need to lock/fetch as nothing should have changed
          data.StreamStatus = 'ACTIVE'
          data.Shards = shards

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



