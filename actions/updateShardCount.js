let BigNumber = require('bignumber.js'),
   db = require('../db'),
   createShards = require('./util.createShards')

module.exports = function UpdateShardCount(store, data, cb) {

  var metaDb = store.metaDb, key = data.StreamName, TargetShardCount = data.TargetShardCount

  metaDb.lock(key, function(release) {
    cb = release(cb)

    store.getStream(key, function(err, stream) {
      if (err) return cb(err)

      if (stream.StreamStatus != 'ACTIVE') {
        return cb(db.clientError('ResourceInUseException',
          'Stream ' + data.StreamName + ' under account ' + metaDb.awsAccountId +
          ' not ACTIVE, instead in state ' + stream.StreamStatus))
      }

      var openShards = stream.Shards.filter(function(shard) {
        return shard.SequenceNumberRange.EndingSequenceNumber == null
      }).length

      if (TargetShardCount > store.shardLimit) {

        return cb(db.clientError('LimitExceededException',
          'Target shard count or number of open shards cannot be greater than ' + store.shardLimit + '. ' +
          'Current open shard count: ' + openShards + ', Target shard count: ' + TargetShardCount))
      }

      if (TargetShardCount > openShards * 2) {

        return cb(db.clientError('LimitExceededException',
          'UpdateShardCount cannot scale up over double your current open shard count. ' +
          'Current open shard count: ' + openShards + ', Target shard count: ' + TargetShardCount))
      }

      if (TargetShardCount < openShards / 2) {
        return cb(db.clientError('LimitExceededException',
          'UpdateShardCount cannot scale down below half your current open shard count. ' +
          'Current open shard count: ' + openShards + ', Target shard count: ' + TargetShardCount))
      }

      if (stream.StreamStatus != 'ACTIVE') {
        return cb(db.clientError('ResourceInUseException',
          'Stream ' + key + ' under account ' + metaDb.awsAccountId +
          ' not ACTIVE, instead in state ' + stream.StreamStatus))
      }

      stream.StreamStatus = 'UPDATING'

      db.sumShards(store, function(err, shardSum) {
        if (err) return cb(err)

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

                stream.Shards.forEach(function(shard, index) {

                  var now = Date.now()

                  shard.SequenceNumberRange.EndingSequenceNumber = db.stringifySequence({
                    shardCreateTime: db.parseSequence(shard.SequenceNumberRange.StartingSequenceNumber).shardCreateTime,
                    shardIx: index,
                    seqIx: new BigNumber('7fffffffffffffff', 16).toFixed(),
                    seqTime: now,
                  })

                })

                stream.Shards = stream.Shards.concat(createShards(TargetShardCount, shardSum))
                stream.StreamStatus = 'ACTIVE'

                metaDb.put(key, stream, cb)
              })
            })

          }, store.updateStreamMs)

          cb(null, { StreamName: key, CurrentShardCount: shardSum, TargetShardCount: data.TargetShardCount })
        })
      })
    })
  })
}
