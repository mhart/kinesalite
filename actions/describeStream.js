var db = require('../db')

module.exports = function describeStream(data, cb) {

  db.getStream(data.StreamName, false, function(err, stream) {
    if (err) return cb(err)

    cb(null, {StreamDescription: stream})
  })
}



