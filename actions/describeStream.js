
module.exports = function describeStream(store, data, cb) {

  store.getStream(data.StreamName, false, function(err, stream) {
    if (err) return cb(err)

    delete stream._seqIx

    cb(null, {StreamDescription: stream})
  })
}



