var db = require('../db')

module.exports = function getShardIterator(store, data, cb) {

  var metaDb = store.metaDb, shardInfo, shardId, shardIx

  try {
    shardInfo = db.resolveShardId(data.ShardId)
  } catch (e) {
    return cb(db.clientError('ResourceNotFoundException',
      'Could not find shard ' + data.ShardId + ' in stream ' + data.StreamName +
      ' under account ' + metaDb.awsAccountId + '.'))
  }
  shardId = shardInfo.shardId
  shardIx = shardInfo.shardIx

  store.getStream(data.StreamName, function(err, stream) {
    if (err) {
      if (err.name == 'NotFoundError' && err.body) {
        err.body.message = 'Shard ' + shardId + ' in stream ' + data.StreamName +
          ' under account ' + metaDb.awsAccountId + ' does not exist'
      }
      return cb(err)
    }

    if (shardIx >= stream.Shards.length) {
      return cb(db.clientError('ResourceNotFoundException',
        'Shard ' + shardId + ' in stream ' + data.StreamName +
        ' under account ' + metaDb.awsAccountId + ' does not exist'))
    }

    var seqObj, seqStr, iteratorSeq, shardSeq = stream.Shards[shardIx].SequenceNumberRange.StartingSequenceNumber,
      shardSeqObj = db.parseSequence(shardSeq)

    if (data.StartingSequenceNumber) {
      if (data.ShardIteratorType == 'TRIM_HORIZON' || data.ShardIteratorType == 'LATEST') {
        return cb(db.clientError('InvalidArgumentException',
          'Must either specify (1) AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER and StartingSequenceNumber or ' +
          '(2) TRIM_HORIZON or LATEST and no StartingSequenceNumber. ' +
          'Request specified ' + data.ShardIteratorType + ' and also a StartingSequenceNumber.'))
      }
      try {
        seqObj = db.parseSequence(data.StartingSequenceNumber)
      } catch (e) {
        return cb(db.clientError('InvalidArgumentException',
          'StartingSequenceNumber ' + data.StartingSequenceNumber + ' used in GetShardIterator on shard ' + shardId +
          ' in stream ' + data.StreamName + ' under account ' + metaDb.awsAccountId + ' is invalid.'))
      }
      if (seqObj.shardIx != shardIx) {
        return cb(db.clientError('InvalidArgumentException',
          'Invalid StartingSequenceNumber. It encodes ' + db.shardIdName(seqObj.shardIx) +
          ', while it was used in a call to a shard with ' + shardId))
      }
      if (seqObj.version != shardSeqObj.version || seqObj.shardCreateTime != shardSeqObj.shardCreateTime) {
        if (seqObj.version === 0) {
          return cb(db.serverError())
        }
        return cb(db.clientError('InvalidArgumentException',
          'StartingSequenceNumber ' + data.StartingSequenceNumber + ' used in GetShardIterator on shard ' + shardId +
          ' in stream ' + data.StreamName + ' under account ' + metaDb.awsAccountId + ' is invalid ' +
          'because it did not come from this stream.'))
      }
      if (data.ShardIteratorType == 'AT_SEQUENCE_NUMBER') {
        iteratorSeq = data.StartingSequenceNumber
      } else { // AFTER_SEQUENCE_NUMBER
        iteratorSeq = db.incrementSequence(seqObj)
      }

      return cb(null, {ShardIterator: db.createShardIterator(data.StreamName, shardId, iteratorSeq)})
    } else if (data.ShardIteratorType == 'TRIM_HORIZON') {
      iteratorSeq = shardSeq
      return cb(null, {ShardIterator: db.createShardIterator(data.StreamName, shardId, iteratorSeq)})
    } else if (data.ShardIteratorType == 'LATEST') {
      iteratorSeq = db.stringifySequence({
        shardCreateTime: shardSeqObj.shardCreateTime,
        seqIx: stream._seqIx[Math.floor(shardIx / 5)],
        seqTime: Date.now(),
        shardIx: shardSeqObj.shardIx,
      })
      return cb(null, {ShardIterator: db.createShardIterator(data.StreamName, shardId, iteratorSeq)})
    } else if (data.ShardIteratorType == 'AT_TIMESTAMP') {
      if (isNaN(data.Timestamp)) {
        return cb(db.clientError('InvalidArgumentException',
          'Must specify timestampInMillis parameter for iterator of type AT_TIMESTAMP. Current request has no timestamp parameter.'))
      }
      opts = {
        gt: db.shardIxToHex(shardIx),
        lt: db.shardIxToHex(shardIx + 1),
      }
      db.lazy(store.getStreamDb(data.StreamName).createReadStream(opts), cb)
        .filter(function(item) { return item.value.ApproximateArrivalTimestamp >= data.Timestamp })
        .head(function(item) {
          cb(null, {ShardIterator: db.createShardIterator(data.StreamName, shardId, item.key.split('/')[1])})
        })
    } else {
      return cb(db.clientError('InvalidArgumentException',
        'Must either specify (1) AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER and StartingSequenceNumber or ' +
        '(2) TRIM_HORIZON or LATEST and no StartingSequenceNumber. ' +
        'Request specified ' + data.ShardIteratorType + ' and no StartingSequenceNumber.'))
    }
  })
}

