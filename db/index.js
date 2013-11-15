var lazy = require('lazy'),
    levelup = require('levelup'),
    MemDown = require('memdown'),
    sublevel = require('level-sublevel'),
    deleteStream = require('level-delete-stream'),
    Lock = require('lock')

var db = sublevel(levelup('./mydb', {db: function(location) { return new MemDown(location) }})),
    metaDb = db.sublevel('meta', {valueEncoding: 'json'}),
    streamDbs = []

exports.createStreamMs = 500
exports.deleteStreamMs = 500
exports.lazy = lazyStream
exports.metaDb = metaDb
exports.getStreamDb = getStreamDb
exports.deleteStreamDb = deleteStreamDb
exports.getStream = getStream
exports.validationError = validationError
exports.checkConditional = checkConditional

metaDb.lock = new Lock()

function getStreamDb(name) {
  if (!streamDbs[name]) {
    streamDbs[name] = db.sublevel('stream-' + name, {valueEncoding: 'json'})
    streamDbs[name].lock = new Lock()
  }
  return streamDbs[name]
}

function deleteStreamDb(name, cb) {
  var streamDb = streamDbs[name] || db.sublevel('stream-' + name, {valueEncoding: 'json'})
  delete streamDbs[name]
  itemDb.createKeyStream().pipe(deleteStream(db, cb))
}

function getStream(name, checkStatus, cb) {
  if (typeof checkStatus == 'function') cb = checkStatus

  streamDb.get(name, function(err, stream) {
    if (!err && checkStatus && (stream.StreamStatus == 'CREATING' || stream.StreamStatus == 'DELETING')) {
      err = new Error('NotFoundError')
      err.name = 'NotFoundError'
    }
    if (err) {
      if (err.name == 'NotFoundError') {
        err.statusCode = 400
        err.body = {
          __type: 'com.amazonaws.kinesis.v20130901#ResourceNotFoundException',
          message: 'Requested resource not found',
        }
        if (!checkStatus) err.body.message += ': Stream: ' + name + ' not found'
      }
      return cb(err)
    }

    cb(null, stream)
  })
}

function lazyStream(stream, errHandler) {
  if (errHandler) stream.on('error', errHandler)
  return lazy(stream)
}

function validationError(msg) {
  if (msg == null) msg = 'The provided key element does not match the schema'
  var err = new Error(msg)
  err.statusCode = 400
  err.body = {
    __type: 'com.amazon.coral.validate#ValidationException',
    message: msg,
  }
  return err
}

function conditionalError(msg) {
  if (msg == null) msg = 'The conditional request failed'
  var err = new Error(msg)
  err.statusCode = 400
  err.body = {
    __type: 'com.amazonaws.kinesis.v20130901#ConditionalCheckFailedException',
    message: msg,
  }
  return err
}

