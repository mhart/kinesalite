var BigNumber = require('bignumber.js'),
    db = require('../db')

var POW_128 = new BigNumber(2).pow(128),
    SEQ_ADJUST_MS = 2000

module.exports = function createStream(store, data, cb) {

  var key = data.StreamName, metaDb = store.metaDb

  metaDb.lock(key, function(release) {
    cb = release(cb)

    metaDb.get(key, function(err) {
      if (err && err.name != 'NotFoundError') return cb(err)
      if (!err)
        return cb(db.clientError('ResourceInUseException',
          'Stream ' + key + ' under account ' + metaDb.awsAccountId + ' already exists.'))

      db.sumShards(store, function(err, shardSum) {
        if (err) return cb(err)

        if (shardSum + data.ShardCount > store.shardLimit) {
          return cb(db.clientError('LimitExceededException',
              'This request would exceed the shard limit for the account ' + metaDb.awsAccountId + ' in ' +
              metaDb.awsRegion + '. Current shard count for the account: ' + shardSum +
              '. Limit: ' + store.shardLimit + '. Number of additional shards that would have ' +
              'resulted from this request: ' + data.ShardCount + '. Refer to the AWS Service Limits page ' +
              '(http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) ' +
              'for current limits and how to request higher limits.'))
        }

        var i, shards = new Array(data.ShardCount), shardHash = POW_128.div(data.ShardCount).floor(),
          createTime = Date.now() - SEQ_ADJUST_MS, stream
        for (i = 0; i < data.ShardCount; i++) {
          shards[i] = {
            HashKeyRange: {
              StartingHashKey: shardHash.times(i).toFixed(),
              EndingHashKey: (i < data.ShardCount - 1 ? shardHash.times(i + 1) : POW_128).minus(1).toFixed(),
            },
            SequenceNumberRange: {
              StartingSequenceNumber: db.stringifySequence({shardCreateTime: createTime, shardIx: i}),
            },
            ShardId: 'shardId-' + ('00000000000' + i).slice(-12)
          }
        }
        stream = {
          HasMoreShards: false,
          Shards: [],
          StreamARN: 'arn:aws:kinesis:' + metaDb.awsRegion + ':' + metaDb.awsAccountId + ':stream/' + data.StreamName,
          StreamName: data.StreamName,
          StreamStatus: 'CREATING',
          _seqIx: new Array(Math.ceil(data.ShardCount / 5)), // Hidden data, remove when returning
          _tags: Object.create(null), // Hidden data, remove when returning
        }

        metaDb.put(key, stream, function(err) {
          if (err) return cb(err)

          setTimeout(function() {

            // Shouldn't need to lock/fetch as nothing should have changed
            stream.StreamStatus = 'ACTIVE'
            stream.Shards = shards

            metaDb.put(key, stream, function(err) {
              // TODO: Need to check this
              if (err) console.error(err)
            })

          }, store.createStreamMs)

          cb()
        })
      })
    })
  })

}
