
module.exports = function describeStreamSummary(store, data, cb) {

  store.getStream(data.StreamName, function(err, stream) {
    if (err) return cb(err)

    stream.OpenShardCount = stream.Shards.filter(function(shard) {
      return shard.SequenceNumberRange.EndingSequenceNumber == null
    }).length

    delete stream._seqIx
    delete stream._tags
    delete stream.Shards
    delete stream.HasMoreShards

    stream.ConsumerCount = 0

    cb(null, {StreamDescriptionSummary: stream})
  })
}
