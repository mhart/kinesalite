var crypto = require('crypto'),
    db = require('../db')

module.exports = function getShardIterator(store, data, cb) {

  var metaDb = store.metaDb, shardId = data.ShardId, shardIx

  shardId = shardId.split('-')[1] || shardId
  shardIx = /^\d+$/.test(shardId) ? parseInt(shardId) : NaN
  if (!(shardIx >= 0 && shardIx <= 2147483647)) {
    if (shardIx) {
      return cb(db.clientError('ResourceNotFoundException',
        'Could not find shard ' + data.ShardId + ' in stream ' + data.StreamName +
        ' under account ' + metaDb.awsAccountId + '.'))
    } else {
      return cb(db.serverError())
    }
  }
  shardId = 'shardId-' + ('00000000000' + shardIx).slice(-12)

  store.getStream(data.StreamName, false, function(err, stream) {
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
          'Invalid StartingSequenceNumber. It encodes shardId-' + ('00000000000' + seqObj.shardIx).slice(-12) +
          ', while it was used in a call to a shard with ' + shardId))
      }
      if (seqObj.version != shardSeqObj.version || seqObj.shardCreateTime != shardSeqObj.shardCreateTime) {
        seqStr = seqObj.version === 0 ? db.stringifySequence(seqObj) : data.StartingSequenceNumber
        return cb(db.clientError('InvalidArgumentException',
          'StartingSequenceNumber ' + seqStr + ' used in GetShardIterator on shard ' + shardId +
          ' in stream ' + data.StreamName + ' under account ' + metaDb.awsAccountId + ' is invalid ' +
          'because it did not come from this stream.'))
      }
      if (data.ShardIteratorType == 'AT_SEQUENCE_NUMBER') {
        iteratorSeq = data.StartingSequenceNumber
      } else {
        iteratorSeq = db.stringifySequence({
          shardCreateTime: seqObj.shardCreateTime,
          seqIx: seqObj.seqIx + 1,
          byte1: seqObj.byte1,
          seqTime: seqObj.seqTime,
          shardIx: seqObj.shardIx,
        })
      }

    } else {
      if (data.ShardIteratorType == 'TRIM_HORIZON') {
        iteratorSeq = shardSeq
      } else if (data.ShardIteratorType == 'LATEST') {
        iteratorSeq = db.stringifySequence({
          shardCreateTime: shardSeqObj.shardCreateTime,
          seqIx: stream._seqIx[Math.floor(shardIx / 5)],
          byte1: shardSeqObj.byte1,
          seqTime: Date.now(),
          shardIx: shardSeqObj.shardIx,
        })
      } else {
        return cb(db.clientError('InvalidArgumentException',
          'Must either specify (1) AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER and StartingSequenceNumber or ' +
          '(2) TRIM_HORIZON or LATEST and no StartingSequenceNumber. ' +
          'Request specified ' + data.ShardIteratorType + ' and no StartingSequenceNumber.'))
      }
    }

    // Unsure how shard iterators are encoded
    // First eight bytes are always [0, 0, 0, 0, 0, 0, 0, 1] (perhaps version number?)
    // Remaining bytes are 16 byte aligned – perhaps AES encrypted?

    // Length depends on name length, given below calculation:
    // 152 + (Math.floor((data.StreamName.length + 2) / 16) * 16)

    var encryptStr = [
      (new Array(14).join('0') + Date.now()).slice(-14),
      data.StreamName,
      shardId,
      iteratorSeq,
      new Array(53).join('0'), // Not entirely sure what would be making up all this data in production
    ].join('/')

    var buffer = Buffer.concat([
      new Buffer([0, 0, 0, 0, 0, 0, 0, 1]),
      crypto.createCipher('aes256', db.ITERATOR_PWD).update(encryptStr, 'utf8'),
    ])

    cb(null, {ShardIterator: buffer.toString('base64')})
  })
}

