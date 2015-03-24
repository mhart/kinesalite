
module.exports = function deleteStream(store, data, cb) {

  var key = data.StreamName, metaDb = store.metaDb

  store.getStream(key, function(err, stream) {
    if (err) return cb(err)

    stream.StreamStatus = 'DELETING'

    metaDb.put(key, stream, function(err) {
      if (err) return cb(err)

      store.deleteStreamDb(key, function(err) {
        if (err) return cb(err)

        setTimeout(function() {
          metaDb.del(key, function(err) {
            // TODO: Need to check this
            if (err) console.error(err)
          })
        }, store.deleteStreamMs)

        cb()
      })
    })
  })

}



