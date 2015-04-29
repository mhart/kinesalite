var BigNumber = require('bignumber.js'),
    db = require('../db')

module.exports = function splitShard(store, data, cb) {

  var metaDb = store.metaDb, key = data.StreamName, shardInfo, shardId, shardIx

  try {
    shardInfo = db.resolveShardId(data.ShardToSplit)
  } catch (e) {
    return cb(db.clientError('ResourceNotFoundException',
      'Could not find shard ' + data.ShardToSplit + ' in stream ' + key +
      ' under account ' + metaDb.awsAccountId + '.'))
  }
  shardId = shardInfo.shardId
  shardIx = shardInfo.shardIx

  metaDb.lock(key, function(release) {
    cb = release(cb)

    store.getStream(key, function(err, stream) {
      if (err) return cb(err)

      if (shardIx >= stream.Shards.length) {
        return cb(db.clientError('ResourceNotFoundException',
          'Could not find shard ' + shardId + ' in stream ' + key +
            ' under account ' + metaDb.awsAccountId + '.'))
      }

      db.sumShards(store, function(err, shardSum) {
        if (err) return cb(err)

        if (shardSum + 1 > store.shardLimit) {
          return cb(db.clientError('LimitExceededException',
              'This request would exceed the shard limit for the account ' + metaDb.awsAccountId + ' in ' +
              metaDb.awsRegion + '. Current shard count for the account: ' + shardSum +
              '. Limit: ' + store.shardLimit + '. Number of additional shards that would have ' +
              'resulted from this request: ' + 1 + '. Refer to the AWS Service Limits page ' +
              '(http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) ' +
              'for current limits and how to request higher limits.'))
        }

        var hashKey = new BigNumber(data.NewStartingHashKey), shard = stream.Shards[shardIx]

        if (hashKey.lte(new BigNumber(shard.HashKeyRange.StartingHashKey).plus(1)) ||
            hashKey.gte(shard.HashKeyRange.EndingHashKey)) {
          return cb(db.clientError('InvalidArgumentException',
            'NewStartingHashKey ' + data.NewStartingHashKey + ' used in SplitShard() on shard ' + shardId +
            ' in stream ' + key + ' under account ' + metaDb.awsAccountId +
            ' is not both greater than one plus the shard\'s StartingHashKey ' +
            shard.HashKeyRange.StartingHashKey + ' and less than the shard\'s EndingHashKey ' +
            shard.HashKeyRange.EndingHashKey + '.'))
        }

        if (stream.StreamStatus != 'ACTIVE') {
          return cb(db.clientError('ResourceInUseException',
            'Stream ' + key + ' under account ' + metaDb.awsAccountId +
            ' not ACTIVE, instead in state ' + stream.StreamStatus))
        }

        stream.StreamStatus = 'UPDATING'

        metaDb.put(key, stream, function(err) {
          if (err) return cb(err)

          setTimeout(function() {

            metaDb.lock(key, function(release) {
              cb = release(function(err) {
                if (err && !/Database is not open/.test(err)) console.error(err.stack || err)
              })

              store.getStream(key, function(err, stream) {
                if (err && err.name == 'NotFoundError') return cb()
                if (err) return cb(err)

                var now = Date.now()

                shard = stream.Shards[shardIx]

                stream.StreamStatus = 'ACTIVE'

                shard.SequenceNumberRange.EndingSequenceNumber = db.stringifySequence({
                  shardCreateTime: db.parseSequence(shard.SequenceNumberRange.StartingSequenceNumber).shardCreateTime,
                  shardIx: shardIx,
                  seqIx: new BigNumber('7fffffffffffffff', 16).toString(),
                  seqTime: now,
                })

                stream.Shards.push({
                  ParentShardId: shardId,
                  HashKeyRange: {
                    StartingHashKey: shard.HashKeyRange.StartingHashKey,
                    EndingHashKey: hashKey.minus(1).toString(),
                  },
                  SequenceNumberRange: {
                    StartingSequenceNumber: db.stringifySequence({
                      shardCreateTime: now + 1000,
                      shardIx: stream.Shards.length
                    }),
                  },
                  ShardId: 'shardId-' + ('00000000000' + stream.Shards.length).slice(-12)
                })

                stream.Shards.push({
                  ParentShardId: shardId,
                  HashKeyRange: {
                    StartingHashKey: hashKey.toString(),
                    EndingHashKey: shard.HashKeyRange.EndingHashKey,
                  },
                  SequenceNumberRange: {
                    StartingSequenceNumber: db.stringifySequence({
                      shardCreateTime: now + 1000,
                      shardIx: stream.Shards.length
                    }),
                  },
                  ShardId: 'shardId-' + ('00000000000' + stream.Shards.length).slice(-12)
                })

                metaDb.put(key, stream, cb)
              })
            })

          }, store.updateStreamMs)

          cb()
        })
      })
    })
  })
}


