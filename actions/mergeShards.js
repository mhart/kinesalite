var BigNumber = require('bignumber.js'),
    db = require('../db')

module.exports = function mergeShards(store, data, cb) {

  var metaDb = store.metaDb, key = data.StreamName, shardNames = [data.ShardToMerge, data.AdjacentShardToMerge],
    shardInfo, shardIds = [], shardIxs = [], i

  for (i = 0; i < shardNames.length; i++) {
    try {
      shardInfo = db.resolveShardId(shardNames[i])
    } catch (e) {
      return cb(db.clientError('ResourceNotFoundException',
        'Could not find shard ' + shardNames[i] + ' in stream ' + key +
        ' under account ' + metaDb.awsAccountId + '.'))
    }
    shardIds[i] = shardInfo.shardId
    shardIxs[i] = shardInfo.shardIx
  }

  metaDb.lock(key, function(release) {
    cb = release(cb)

    store.getStream(key, function(err, stream) {
      if (err) return cb(err)

      for (i = 0; i < shardIxs.length; i++) {
        if (shardIxs[i] >= stream.Shards.length) {
          return cb(db.clientError('ResourceNotFoundException',
            'Could not find shard ' + shardIds[i] + ' in stream ' + key +
              ' under account ' + metaDb.awsAccountId + '.'))
        }
      }

      var shards = [stream.Shards[shardIxs[0]], stream.Shards[shardIxs[1]]]

      if (!new BigNumber(shards[0].HashKeyRange.EndingHashKey).plus(1).eq(shards[1].HashKeyRange.StartingHashKey)) {
        return cb(db.clientError('InvalidArgumentException',
          'Shards ' + shardIds[0] + ' and ' + shardIds[1] + ' in stream ' + key +
          ' under account ' + metaDb.awsAccountId + ' are not an adjacent pair of shards eligible for merging'))
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
              if (err) console.error(err)
            })

            store.getStream(key, function(err, stream) {
              if (err && err.name == 'NotFoundError') return cb()
              if (err) return cb(err)

              var now = Date.now()

              var shards = [stream.Shards[shardIxs[0]], stream.Shards[shardIxs[1]]]

              stream.StreamStatus = 'ACTIVE'

              shards[0].SequenceNumberRange.EndingSequenceNumber = db.stringifySequence({
                shardCreateTime: db.parseSequence(shards[0].SequenceNumberRange.StartingSequenceNumber).shardCreateTime,
                shardIx: shardIxs[0],
                seqIx: new BigNumber('7fffffffffffffff', 16).toString(),
                seqTime: now,
              })

              shards[1].SequenceNumberRange.EndingSequenceNumber = db.stringifySequence({
                shardCreateTime: db.parseSequence(shards[1].SequenceNumberRange.StartingSequenceNumber).shardCreateTime,
                shardIx: shardIxs[1],
                seqIx: new BigNumber('7fffffffffffffff', 16).toString(),
                seqTime: now,
              })

              stream.Shards.push({
                ParentShardId: shardIds[0],
                AdjacentParentShardId: shardIds[1],
                HashKeyRange: {
                  StartingHashKey: shards[0].HashKeyRange.StartingHashKey,
                  EndingHashKey: shards[1].HashKeyRange.EndingHashKey,
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
}

