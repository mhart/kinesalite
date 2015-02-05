var BigNumber = require('bignumber.js'),
    db = require('../db')

module.exports = function putRecord(store, data, cb) {

  var key = data.StreamName, metaDb = store.metaDb, streamDb = store.getStreamDb(data.StreamName)

  metaDb.lock(key, function(release) {
    cb = release(cb)

    store.getStream(data.StreamName, false, function(err, stream) {
      if (err) return cb(err)

      var hashKey, shardIx, shardId, streamCreateTime

      if (data.ExplicitHashKey != null) {
        hashKey = BigNumber(data.ExplicitHashKey)

        if (hashKey.cmp(0) < 0 || hashKey.cmp(BigNumber(2).pow(128)) >= 0) {
          return cb(db.clientError('InvalidArgumentException',
            'Invalid ExplicitHashKey. ExplicitHashKey must be in the range: [0, 2^128-1]. ' +
            'Specified value was ' + data.ExplicitHashKey))
        }
      } else {
        hashKey = db.partitionKeyToHashKey(data.PartitionKey)
      }

      if (data.SequenceNumberForOrdering != null) {
        var hex = BigNumber(data.SequenceNumberForOrdering).toString(16)
        if (hex[0] != '2' || parseInt(hex[11], 16) > 7 || hex.slice(27, 30) != '000' || parseInt(hex[38], 16) > 7 ||
            parseInt(hex.slice(30, 38), 16) > Math.floor(Date.now() / 1000) || !~['1', '2', '3'].indexOf(hex[46])) {
          err = new Error
          err.statusCode = 400
          err.body = {
            __type: 'InvalidArgumentException',
            message: 'ExclusiveMinimumSequenceNumber ' + data.SequenceNumberForOrdering +
              ' used in PutRecord on stream ' + data.StreamName +
              ' under account ' + store.metaDb.awsAccountId + ' is invalid.',
          }
          return cb(err)
        }
        if (hex[46] == '3' || (parseInt(hex[19], 16) > 7 && parseInt(hex.slice(30, 38), 16) > 0)) {
          err = new Error
          err.statusCode = 500
          err.body = {
            __type: 'InternalFailure',
          }
          return cb(err)
        }
      }

      for (var i = 0; i < stream.Shards.length; i++) {
        if (hashKey.cmp(stream.Shards[i].HashKeyRange.StartingHashKey) >= 0 &&
            hashKey.cmp(stream.Shards[i].HashKeyRange.EndingHashKey) <= 0) {
          shardIx = i
          shardId = stream.Shards[i].ShardId
          streamCreateTime = db.parseSequence(
            stream.Shards[i].SequenceNumberRange.StartingSequenceNumber).streamCreateTime
          break
        }
      }

      var seqIxIx = Math.floor(shardIx / 5)

      var seqNum = db.stringifySequence({
        streamCreateTime: streamCreateTime,
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
