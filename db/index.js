var lazy = require('lazy'),
    levelup = require('levelup'),
    memdown = require('memdown'),
    sublevel = require('level-sublevel'),
    Lock = require('lock')

exports.create = create
exports.lazy = lazyStream
exports.validationError = validationError

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

