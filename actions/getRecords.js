var crypto = require('crypto'),
    once = require('once'),
    db = require('../db')

module.exports = function getRecords(store, data, cb) {

  var metaDb = store.metaDb, shardIx, shardId, iteratorTime, streamName, seqNo, seqObj, pieces,
    buffer = new Buffer(data.ShardIterator, 'base64'), now = Date.now(),
    decipher = crypto.createDecipher('aes-256-cbc', db.ITERATOR_PWD)

  if (buffer.length < 152 || buffer.length > 280 || buffer.toString('base64') != data.ShardIterator)
    return cb(invalidShardIterator())

  if (buffer.slice(0, 8).toString('hex') != '0000000000000001')
    return cb(invalidShardIterator())

  try {
    pieces = Buffer.concat([decipher.update(buffer.slice(8)), decipher.final()]).toString('utf8').split('/')
  } catch (e) {
    return cb(invalidShardIterator())
  }

  if (pieces.length != 5)
    return cb(invalidShardIterator())

  iteratorTime = +pieces[0]
  streamName = pieces[1]
  shardId = pieces[2]
  seqNo = pieces[3]

  shardIx = parseInt(shardId.split('-')[1])
  if (!/^shardId-[\d]{12}$/.test(shardId) || !(shardIx >= 0 && shardIx < 2147483648))
    return cb(invalidShardIterator())

  if (!(iteratorTime > 0 && iteratorTime < Date.now()))
    return cb(invalidShardIterator())

  if (!/[a-zA-Z0-9_.-]+/.test(streamName) || !streamName.length || streamName.length > 128)
    return cb(invalidShardIterator())

  if ((now - iteratorTime) > 300000) {
    return cb(db.clientError('ExpiredIteratorException',
      'Iterator expired. The iterator was created at time ' + toAmzUtcString(iteratorTime) +
      ' while right now it is ' + toAmzUtcString(now) + ' which is further in the future than the ' +
      'tolerated delay of 300000 milliseconds.'))
  }

  try {
    seqObj = db.parseSequence(seqNo)
  } catch (e) {
    return cb(invalidShardIterator())
  }

  store.getStream(streamName, function(err, stream) {
    if (err) {
      if (err.name == 'NotFoundError' && err.body) {
        err.body.message = 'Shard ' + shardId + ' in stream ' + streamName +
          ' under account ' + metaDb.awsAccountId + ' does not exist'
      }
      return cb(err)
    }
    if (shardIx >= stream.Shards.length) {
      return cb(db.clientError('ResourceNotFoundException',
        'Shard ' + shardId + ' in stream ' + streamName +
        ' under account ' + metaDb.awsAccountId + ' does not exist'))
    }

    cb = once(cb)

    var streamDb = store.getStreamDb(streamName), cutoffTime = Date.now() - (24 * 60 * 60 * 1000),
      keysToDelete = [], lastItem, opts

    opts = {
      gte: db.shardIxToHex(shardIx) + '/' + seqNo,
      lt: db.shardIxToHex(shardIx + 1),
    }

    db.lazy(streamDb.createReadStream(opts), cb)
      .take(data.Limit || 10000)
      .map(function(item) {
        lastItem = item.value
        lastItem.SequenceNumber = item.key.split('/')[1]
        lastItem._seqObj = db.parseSequence(lastItem.SequenceNumber)
        lastItem._tooOld = lastItem._seqObj.seqTime < cutoffTime
        if (lastItem._tooOld) keysToDelete.push(item.key)
        return lastItem
      })
      .filter(function(item) { return !item._tooOld })
      .join(function(items) {
        var nextSeq = lastItem ? db.incrementSequence(lastItem._seqObj) : seqNo,
          nextShardIterator = db.createShardIterator(streamName, shardId, nextSeq)

        cb(null, {
          NextShardIterator: nextShardIterator,
          Records: items.map(function(item) {
            delete item._seqObj
            delete item._tooOld
            return item
          }),
        })

        if (keysToDelete.length) {
          // Do this async
          streamDb.batch(keysToDelete.map(function(key) { return {type: 'del', key: key} }), function(err) {
            if (err) console.error(err)
          })
        }
      })
  })
}

function invalidShardIterator() {
  return db.clientError('InvalidArgumentException', 'Invalid ShardIterator.')
}

// Thu Jan 22 01:22:02 UTC 2015
function toAmzUtcString(date) {
  var pieces = new Date(date).toUTCString().match(/^(.+), (.+) (.+) (.+) (.+) GMT$/)
  return [pieces[1], pieces[3], pieces[2], pieces[5], 'UTC', pieces[4]].join(' ')
}
