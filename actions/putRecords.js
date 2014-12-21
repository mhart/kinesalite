
module.exports = function putRecords(store, data, cb) {

  store.getStream(data.StreamName, false, function(err, stream) {
    if (err) return cb(err)

    cb()
  })
}

