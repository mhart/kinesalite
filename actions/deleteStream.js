var db = require('../db'),
    metaDb = db.metaDb

module.exports = function deleteStream(data, cb) {

  var key = data.StreamName

  db.getStream(key, false, function(err, stream) {
    if (err) return cb(err)

    // Check if stream is ACTIVE or not?
    if (stream.StreamStatus == 'CREATING') {
      err = new Error
      err.statusCode = 400
      err.body = {
        __type: 'com.amazonaws.kinesis.v20130901#ResourceInUseException',
        message: 'Attempt to change a resource which is still in use: Stream is being created: ' + key,
      }
      return cb(err)
    }

    stream.StreamStatus = 'DELETING'

    metaDb.put(key, stream, function(err) {
      if (err) return cb(err)

      db.deleteStreamDb(key, function(err) {
        if (err) return cb(err)

        setTimeout(function() {
          metaDb.del(key, function(err) {
            // TODO: Need to check this
            if (err) console.error(err)
          })
        }, db.deleteStreamMs)

        cb()
      })
    })
  })

}



