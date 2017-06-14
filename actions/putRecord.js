var BigNumber = require('bignumber.js'),
    db = require('../db')

module.exports = function putRecord(store, data, cb) {

  var key = data.StreamName, metaDb = store.metaDb, streamDb = store.getStreamDb(data.StreamName), throughputExceededPercent = store.throughputExceededPercent

  metaDb.lock(key, function(release) {
    cb = release(cb)

    if (throughputExceededPercent > 0) {
      if(Math.floor(Math.random()*100) < throughputExceededPercent) {
        return cb(db.clientError('ProvisionedThroughputExceededException', 'Rate exceeded for shard shardId-000000000000 in stream ' + data.StreamName + ' under account ' + metaDb.awsAccountId + ' .'))
      }
    }
    store.getStream(data.StreamName, function(err, stream) {
      if (err) return cb(err)

      if (!~['ACTIVE', 'UPDATING'].indexOf(stream.StreamStatus)) {
        return cb(db.clientError('ResourceNotFoundException',
          'Stream ' + data.StreamName + ' under account ' + metaDb.awsAccountId + ' not found.'))
      }

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
        if (stream.Shards[i].SequenceNumberRange.EndingSequenceNumber == null &&
            hashKey.cmp(stream.Shards[i].HashKeyRange.StartingHashKey) >= 0 &&
            hashKey.cmp(stream.Shards[i].HashKeyRange.EndingHashKey) <= 0) {
          shardIx = i
          shardId = stream.Shards[i].ShardId
          shardCreateTime = db.parseSequence(
            stream.Shards[i].SequenceNumberRange.StartingSequenceNumber).shardCreateTime
          break
        }
      }

      var seqIxIx = Math.floor(shardIx / 5), now = Math.max(Date.now(), shardCreateTime)

      // Ensure that the first record will always be above the stream start sequence
      if (!stream._seqIx[seqIxIx])
        stream._seqIx[seqIxIx] = shardCreateTime == now ? 1 : 0

      var seqNum = db.stringifySequence({
        shardCreateTime: shardCreateTime,
        shardIx: shardIx,
        seqIx: stream._seqIx[seqIxIx],
        seqTime: now,
      })

      var streamKey = db.shardIxToHex(shardIx) + '/' + seqNum

      stream._seqIx[seqIxIx]++

      metaDb.put(key, stream, function(err) {
        if (err) return cb(err)

        var record = {
          PartitionKey: data.PartitionKey,
          Data: data.Data,
          ApproximateArrivalTimestamp: now / 1000,
        }

        streamDb.put(streamKey, record, function(err) {
          if (err) return cb(err)
          cb(null, {ShardId: shardId, SequenceNumber: seqNum})
        })
      })
    })
  })
}
