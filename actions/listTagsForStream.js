
module.exports = function listTagsForStream(store, data, cb) {

  store.getStream(data.StreamName, function(err, stream) {
    if (err) return cb(err)

    var hasMoreTags, limit = data.Limit || 100, keys = Object.keys(stream._tags).sort(), tags

    if (data.ExclusiveStartTagKey)
      keys = keys.filter(function(key) { return key > data.ExclusiveStartTagKey })

    hasMoreTags = keys.length > limit
    tags = keys.slice(0, limit).map(function(key) { return {Key: key, Value: stream._tags[key]} })

    cb(null, {Tags: tags, HasMoreTags: hasMoreTags})
  })
}



