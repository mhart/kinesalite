
module.exports = function describeStream(store, data, cb) {

  store.getStream(data.StreamName, function(err, stream) {
    if (err) return cb(err)

    delete stream._seqIx
    delete stream._tags

    cb(null, {StreamDescription: stream})
  })
}



