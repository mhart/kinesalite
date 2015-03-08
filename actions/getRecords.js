var crypto = require('crypto'),
    db = require('../db')

module.exports = function getRecords(store, data, cb) {

  var metaDb = store.metaDb, shardIx, shardId, iteratorTime, streamName, seqNo, pieces,
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

  store.getStream(streamName, false, function(err, stream) {
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

    cb(null, {NextShardIterator: data.ShardIterator, Records: []})
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
