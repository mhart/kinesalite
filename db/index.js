var crypto = require('crypto'),
    lazy = require('lazy'),
    levelup = require('levelup'),
    memdown = require('memdown'),
    sublevel = require('level-sublevel'),
    Lock = require('lock'),
    BigNumber = require('bignumber.js')

exports.create = create
exports.lazy = lazyStream
exports.validationError = validationError
exports.parseSequence = parseSequence
exports.stringifySequence = stringifySequence
exports.partitionKeyToHashKey = partitionKeyToHashKey

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

function validationError(msg) {
  if (msg == null) msg = 'The provided key element does not match the schema'
  var err = new Error(msg)
  err.statusCode = 400
  err.body = {
    __type: 'ValidationException',
    message: msg,
  }
  return err
}

function parseSequence(seq) {
  var hex = BigNumber(seq).toString(16), version = parseInt(hex.slice(hex.length - 1), 16)
  if (version == 2) {
    return {
      streamCreateTime: new Date(parseInt(hex.slice(2, 10), 16) * 1000),
      shardIx: parseInt(hex.slice(10, 11), 16),
      seqIx: BigNumber(hex.slice(11, 27), 16).toFixed(),
      seqTime: new Date(parseInt(hex.slice(30, 38), 16) * 1000),
      version: version,
    }
  } else if (version == 1) {
    return {
      streamCreateTime: new Date(parseInt(hex.slice(2, 10), 16) * 1000),
      shardIx: parseInt(hex.slice(10, 11), 16),
      seqTime: new Date(parseInt(hex.slice(14, 22), 16) * 1000),
      seqRand: hex.slice(22, 36),
      seqIx: parseInt(hex.slice(36, 38), 16),
      version: version,
    }
  } else {
    throw new Error('Unknown version: ' + version)
  }
}

function stringifySequence(obj) {
  if (!obj.version || obj.version == 2) {
    return BigNumber([
      '20',
      Math.floor(obj.streamCreateTime / 1000).toString(16),
      (obj.shardIx || 0).toString(16),
      ('0000000000000000' + BigNumber(obj.seqIx || 0).toString(16)).slice(-16),
      '000',
      Math.floor((obj.seqTime || obj.streamCreateTime) / 1000).toString(16),
      '0000000',
      (obj.shardIx || 0).toString(16),
      '2',
    ].join(''), 16).toFixed()
  } else if (obj.version == 1) {
    return BigNumber([
      '20',
      Math.floor(obj.streamCreateTime / 1000).toString(16),
      (obj.shardIx || 0).toString(16),
      '000',
      Math.floor((obj.seqTime || obj.streamCreateTime) / 1000).toString(16),
      obj.seqRand || '00000000000000', // Just seems to be a random string of hex
      ('0' + (obj.seqIx || 0).toString(16)).slice(-2),
      '0000000',
      (obj.shardIx || 0).toString(16),
      '1',
    ].join(''), 16).toFixed()
  } else {
    throw new Error('Unknown version: ' + obj.version)
  }
}

// Will determine ExplicitHashKey, which will determine ShardId based on stream's HashKeyRange
function partitionKeyToHashKey(partitionKey) {
  return BigNumber(crypto.createHash('md5').update(partitionKey, 'utf8').digest('hex'), 16)
}
