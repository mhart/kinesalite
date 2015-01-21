var BigNumber = require('bignumber.js'),
    db = require('../db')

var POW_128 = BigNumber(2).pow(128),
    SEQ_ADJUST_MS = 2000

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

      sumShards(metaDb, function(err, shardSum) {
        if (err) return cb(err)

        if (shardSum + data.ShardCount > 10) {
          err = new Error
          err.statusCode = 400
          err.body = {
            __type: 'LimitExceededException',
            message: 'This request would exceed the shard limit for the account ' + metaDb.awsAccountId + ' in us-east-1. ' +
              'Current shard count for the account: ' + shardSum + '. Limit: 10. Number of additional shards that would have ' +
              'resulted from this request: ' + data.ShardCount + '. Refer to the AWS Service Limits page ' +
              '(http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) ' +
              'for current limits and how to request higher limits.',
          }
          return cb(err)
        }

        var i, shards = new Array(data.ShardCount), shardHash = POW_128.div(data.ShardCount).floor(),
          createTime = Date.now() - SEQ_ADJUST_MS, seqIx = new Array(Math.ceil(data.ShardCount / 5))
        for (i = 0; i < data.ShardCount; i++) {
          shards[i] = {
            //ParentShardId: '',
            //AdjacentParentShardId: '',
            HashKeyRange: {
              StartingHashKey: shardHash.times(i).toFixed(),
              EndingHashKey: (i < data.ShardCount - 1 ? shardHash.times(i + 1) : POW_128).minus(1).toFixed(),
            },
            SequenceNumberRange: {
              StartingSequenceNumber: db.stringifySequence({streamCreateTime: createTime, shardIx: i}),
              //EndingSequenceNumber: '49537279973004700513262647557344055618854783026326405121',
            },
            ShardId: 'shardId-' + ('00000000000' + i).slice(-12)
          }
          seqIx[i / 5] = 0
        }
        data = {
          HasMoreShards: false,
          Shards: [],
          StreamARN: 'arn:aws:kinesis:us-east-1:' + metaDb.awsAccountId + ':stream/' + data.StreamName,
          StreamName: data.StreamName,
          StreamStatus: 'CREATING',
          _seqIx: seqIx, // Hidden data, remove when returning
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
  })

}

function sumShards(metaDb, cb) {
  db.lazy(metaDb.createValueStream(), cb)
    .map(function(stream) { return stream.Shards.length })
    .sum(function(sum) { return cb(null, sum) })
}
