var once = require('once'),
    db = require('../db'),
    metaDb = db.metaDb

module.exports = function listStreams(data, cb) {
  cb = once(cb)
  var opts, keys

  if (data.ExclusiveStartStreamName)
    opts = {start: data.ExclusiveStartStreamName + '\x00'}

  keys = db.lazy(metaDb.createKeyStream(opts), cb)

  if (data.Limit) keys = keys.take(data.Limit)

  keys.join(function(names) {
    var result = {StreamNames: names}
    if (data.Limit) result.IsMoreDataAvailable = true // TODO: fix this
    cb(null, result)
  })
}


