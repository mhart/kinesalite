var db = require('../db')

module.exports = function addTagsToStream(store, data, cb) {

  var metaDb = store.metaDb

  metaDb.lock(data.StreamName, function(release) {
    cb = release(cb)

    store.getStream(data.StreamName, false, function(err, stream) {
      if (err) return cb(err)

      var keys = Object.keys(data.Tags), values = keys.map(function(key) { return data.Tags[key] }),
        all = keys.concat(values)

      if (all.some(function(key) { return /[^\u00C0-\u1FFF\u2C00-\uD7FF\w\.\/\-=+_ @%]/.test(key) }))
        return cb(db.clientError('InvalidArgumentException',
          'Some tags contain invalid characters. Valid characters: ' +
          'Unicode letters, digits, white space, _ . / = + - % @.'))

      if (all.some(function(key) { return ~key.indexOf('%') }))
        return cb(db.clientError('InvalidArgumentException',
          'Failed to add tags to stream ' + data.StreamName + ' under account ' + metaDb.awsAccountId +
          ' because some tags contained illegal characters. The allowed characters are ' +
          'Unicode letters, white-spaces, \'_\',\',\',\'/\',\'=\',\'+\',\'-\',\'%\',\'@\'.'))

      var newKeys = keys.concat(Object.keys(stream._tags)).reduce(function(obj, key) {
        obj[key] = true
        return obj
      }, {})

      if (Object.keys(newKeys).length > 10)
        return cb(db.clientError('InvalidArgumentException',
          'Failed to add tags to stream ' + data.StreamName + ' under account ' + metaDb.awsAccountId +
          ' because a given stream cannot have more than 10 tags associated with it.'))

      keys.forEach(function(key) {
        stream._tags[key] = data.Tags[key]
      })

      metaDb.put(data.StreamName, stream, function(err) {
        if (err) return cb(err)

        cb()
      })
    })
  })
}
