var crypto = require('crypto'),
    lazy = require('lazy'),
    levelup = require('levelup'),
    memdown = require('memdown'),
    sublevel = require('level-sublevel'),
    Lock = require('lock'),
    BigNumber = require('bignumber.js')

exports.create = create
exports.lazy = lazyStream
exports.clientError = clientError
exports.serverError = serverError
exports.validationError = validationError
exports.parseSequence = parseSequence
exports.stringifySequence = stringifySequence
exports.partitionKeyToHashKey = partitionKeyToHashKey
exports.ITERATOR_PWD = 'kinesalite'

function create(options) {
  options = options || {}
  options.path = options.path || memdown
  if (options.createStreamMs == null) options.createStreamMs = 500
  if (options.deleteStreamMs == null) options.deleteStreamMs = 500
  if (options.updateStreamMs == null) options.updateStreamMs = 500

  var db = sublevel(levelup(options.path)),
      metaDb = db.sublevel('meta', {valueEncoding: 'json'}),
      streamDbs = []

  metaDb.lock = new Lock()

  // XXX: Is there a better way to get this?
  metaDb.awsAccountId = (process.env.AWS_ACCOUNT_ID || '0000-0000-0000').replace(/[^\d]/g, '')
  metaDb.awsRegion = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1'

  function getStreamDb(name) {
    if (!streamDbs[name]) {
      streamDbs[name] = db.sublevel('stream-' + name, {valueEncoding: 'json'})
      streamDbs[name].lock = new Lock()
    }
    return streamDbs[name]
  }

  function deleteStreamDb(name, cb) {
    var streamDb = getStreamDb(name)
    delete streamDbs[name]
    lazyStream(streamDb.createKeyStream(), cb).join(function(keys) {
      streamDb.batch(keys.map(function(key) { return {type: 'del', key: key} }), cb)
    })
  }

  function getStream(name, checkStatus, cb) {
    if (typeof checkStatus == 'function') cb = checkStatus

    metaDb.get(name, function(err, stream) {
      if (!err && checkStatus && (stream.StreamStatus == 'CREATING' || stream.StreamStatus == 'DELETING')) {
        err = new Error('NotFoundError')
        err.name = 'NotFoundError'
      }
      if (err) {
        if (err.name == 'NotFoundError') {
          err.statusCode = 400
          err.body = {
            __type: 'ResourceNotFoundException',
            message: 'Stream ' + name + ' under account ' + metaDb.awsAccountId + ' not found.',
          }
          //if (!checkStatus) err.body.message += ': Stream: ' + name + ' not found'
        }
        return cb(err)
      }

      cb(null, stream)
    })
  }

  return {
    createStreamMs: options.createStreamMs,
    deleteStreamMs: options.deleteStreamMs,
    updateStreamMs: options.updateStreamMs,
    metaDb: metaDb,
    getStreamDb: getStreamDb,
    deleteStreamDb: deleteStreamDb,
    getStream: getStream,
  }
}

function lazyStream(stream, errHandler) {
  if (errHandler) stream.on('error', errHandler)
  var streamAsLazy = lazy(stream)
  if (stream.destroy) streamAsLazy.on('pipe', stream.destroy.bind(stream))
  return streamAsLazy
}

function clientError(type, msg, statusCode) {
  if (statusCode == null) statusCode = 400
  var err = new Error(msg || type)
  err.statusCode = statusCode
  err.body = {__type: type}
  if (msg != null) err.body.message = msg
  return err
}

function serverError(type, msg, statusCode) {
  return clientError(type || 'InternalFailure', msg, statusCode || 500)
}

function validationError(msg) {
  if (msg == null) msg = 'The provided key element does not match the schema'
  return clientError('ValidationException', msg)
}

var POW_2_124 = BigNumber(2).pow(124)
var POW_2_124_NEXT = BigNumber(2).pow(125).times(16)
var POW_2_185 = BigNumber(2).pow(185)
var POW_2_185_NEXT = BigNumber(2).pow(185).times(1.5)

function parseSequence(seq) {
  var seqNum = BigNumber(seq)
  if (seqNum.lt(POW_2_124)) {
    seqNum = seqNum.plus(POW_2_124)
  }
  var hex = seqNum.toString(16), version = seqNum.lt(POW_2_124_NEXT) ? 0 :
    (seqNum.gt(POW_2_185) && seqNum.lt(POW_2_185_NEXT)) ? parseInt(hex.slice(hex.length - 1), 16) : null
  if (version == 2) {
    var seqIxHex = hex.slice(11, 27), shardIxHex = hex.slice(38, 46)
    if (parseInt(seqIxHex[0], 16) > 7) throw new Error('Sequence index too high')
    if (parseInt(shardIxHex[0], 16) > 7) throw new Error('Shard index too high')
    return {
      shardCreateTime: new Date(parseInt(hex.slice(1, 10), 16) * 1000),
      seqIx: BigNumber(seqIxHex, 16).toFixed(),
      byte1: hex.slice(27, 29),
      seqTime: new Date(parseInt(hex.slice(29, 38), 16) * 1000),
      shardIx: parseInt(shardIxHex, 16),
      version: version,
    }
  } else if (version == 1) {
    return {
      shardCreateTime: new Date(parseInt(hex.slice(1, 10), 16) * 1000),
      byte1: hex.slice(11, 13),
      seqTime: new Date(parseInt(hex.slice(13, 22), 16) * 1000),
      seqRand: hex.slice(22, 36),
      seqIx: parseInt(hex.slice(36, 38), 16),
      shardIx: parseInt(hex.slice(38, 46), 16),
      version: version,
    }
  } else if (version === 0) {
    var shardCreateSecs = parseInt(hex.slice(1, 10), 16)
    if (shardCreateSecs >= 16025175000) throw new Error('Date too large: ' + shardCreateSecs)
    return {
      shardCreateTime: new Date(shardCreateSecs * 1000),
      byte1: hex.slice(10, 12),
      seqRand: hex.slice(12, 28),
      shardIx: parseInt(hex.slice(28, 32), 16),
      version: version,
    }
  } else {
    throw new Error('Unknown version: ' + version)
  }
}

function stringifySequence(obj) {
  if (obj.version == null || obj.version == 2) {
    return BigNumber([
      '2',
      ('00000000' + Math.floor(new Date(obj.shardCreateTime) / 1000).toString(16)).slice(-9),
      (obj.shardIx || 0).toString(16).slice(-1),
      ('0000000000000000' + BigNumber(obj.seqIx || 0).toString(16)).slice(-16),
      obj.byte1 || '00', // Unsure what this is
      ('00000000' + Math.floor(new Date(obj.seqTime || obj.shardCreateTime) / 1000).toString(16)).slice(-9),
      ('0000000' + (obj.shardIx || 0).toString(16)).slice(-8),
      '2',
    ].join(''), 16).toFixed()
  } else if (obj.version == 1) {
    return BigNumber([
      '2',
      ('00000000' + Math.floor(new Date(obj.shardCreateTime) / 1000).toString(16)).slice(-9),
      (obj.shardIx || 0).toString(16).slice(-1),
      obj.byte1 || '00', // Unsure what this is
      ('00000000' + Math.floor(new Date(obj.seqTime || obj.shardCreateTime) / 1000).toString(16)).slice(-9),
      obj.seqRand || '00000000000000', // Just seems to be a random string of hex
      ('0' + (obj.seqIx || 0).toString(16)).slice(-2),
      ('0000000' + (obj.shardIx || 0).toString(16)).slice(-8),
      '1',
    ].join(''), 16).toFixed()
  } else if (obj.version === 0) {
    return BigNumber([
      '1',
      ('00000000' + Math.floor(new Date(obj.shardCreateTime) / 1000).toString(16)).slice(-9),
      obj.byte1 || '00', // Unsure what this is
      obj.seqRand || '0000000000000000', // Unsure what this is
      ('000' + (obj.shardIx || 0).toString(16)).slice(-4),
    ].join(''), 16).toFixed()
  } else {
    throw new Error('Unknown version: ' + obj.version)
  }
}

// Will determine ExplicitHashKey, which will determine ShardId based on stream's HashKeyRange
function partitionKeyToHashKey(partitionKey) {
  return BigNumber(crypto.createHash('md5').update(partitionKey, 'utf8').digest('hex'), 16)
}
