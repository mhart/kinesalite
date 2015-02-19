var BigNumber = require('bignumber.js'),
    db = require('../db')

module.exports = function putRecord(store, data, cb) {

  var key = data.StreamName, metaDb = store.metaDb, streamDb = store.getStreamDb(data.StreamName)

  metaDb.lock(key, function(release) {
    cb = release(cb)

    store.getStream(data.StreamName, false, function(err, stream) {
      if (err) return cb(err)

      var hashKey, shardIx, shardId, shardCreateTime

      if (data.ExplicitHashKey != null) {
        hashKey = new BigNumber(data.ExplicitHashKey)

        if (hashKey.cmp(0) < 0 || hashKey.cmp(new BigNumber(2).pow(128)) >= 0) {
          return cb(db.clientError('InvalidArgumentException',
            'Invalid ExplicitHashKey. ExplicitHashKey must be in the range: [0, 2^128-1]. ' +
            'Specified value was ' + data.ExplicitHashKey))
        }
      } else {
        hashKey = db.partitionKeyToHashKey(data.PartitionKey)
      }

      if (data.SequenceNumberForOrdering != null) {
        try {
          var seqObj = db.parseSequence(data.SequenceNumberForOrdering)
          if (seqObj.seqTime > Date.now()) throw new Error('Sequence time in the future')
        } catch (e) {
          return cb(e.message == 'Unknown version: 3' ? db.serverError() : db.clientError('InvalidArgumentException',
              'ExclusiveMinimumSequenceNumber ' + data.SequenceNumberForOrdering + ' used in PutRecord on stream ' +
              data.StreamName + ' under account ' + metaDb.awsAccountId + ' is invalid.'))
        }
      }

      for (var i = 0; i < stream.Shards.length; i++) {
        if (hashKey.cmp(stream.Shards[i].HashKeyRange.StartingHashKey) >= 0 &&
            hashKey.cmp(stream.Shards[i].HashKeyRange.EndingHashKey) <= 0) {
          shardIx = i
          shardId = stream.Shards[i].ShardId
          shardCreateTime = db.parseSequence(
            stream.Shards[i].SequenceNumberRange.StartingSequenceNumber).shardCreateTime
          break
        }
      }

      var seqIxIx = Math.floor(shardIx / 5)

      var seqNum = db.stringifySequence({
        shardCreateTime: shardCreateTime,
        shardIx: shardIx,
        seqIx: stream._seqIx[seqIxIx],
        seqTime: Date.now(),
      })

      stream._seqIx[seqIxIx]++

      metaDb.put(key, stream, function(err) {
        if (err) return cb(err)

        streamDb.put(seqNum, {PartitionKey: data.PartitionKey, Data: data.Data}, function(err) {
          if (err) return cb(err)
          cb(null, {ShardId: shardId, SequenceNumber: seqNum})
        })
      })
    })
  })
}
