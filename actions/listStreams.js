var once = require('once'),
    db = require('../db')

module.exports = function listStreams(store, data, cb) {
  cb = once(cb)
  var opts, keys

  if (data.ExclusiveStartStreamName)
    opts = {start: data.ExclusiveStartStreamName + '\x00'}

  keys = db.lazy(store.metaDb.createKeyStream(opts), cb)

  if (data.Limit) keys = keys.take(data.Limit)

  keys.join(function(names) {
    var result = {StreamNames: names}
    if (data.Limit) result.IsMoreDataAvailable = true // TODO: fix this
    cb(null, result)
  })
}


