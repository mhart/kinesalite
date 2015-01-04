
module.exports = function removeTagsFromStream(store, data, cb) {

  store.getStream(data.StreamName, false, function(err, stream) {
    if (err) return cb(err)

    cb()
  })
}



