var db = require('../db')

module.exports = function increaseStreamRetentionPeriod(store, data, cb) {

  var metaDb = store.metaDb

  metaDb.lock(data.StreamName, function(release) {
    cb = release(cb)

    store.getStream(data.StreamName, function(err, stream) {
      if (err) return cb(err)

      if (data.RetentionPeriodHours < 24) {
        return cb(db.clientError('InvalidArgumentException',
          'Minimum allowed retention period is 24 hours. Requested retention period (' + data.RetentionPeriodHours +
          ' hours) is too short.'))
      }

      if (data.RetentionPeriodHours > 168) {
        return cb(db.clientError('InvalidArgumentException',
          'Maximum allowed retention period is 168 hours. Requested retention period (' + data.RetentionPeriodHours +
          ' hours) is too long.'))
      }

      if (stream.RetentionPeriodHours > data.RetentionPeriodHours) {
        return cb(db.clientError('InvalidArgumentException',
          'Requested retention period (' + data.RetentionPeriodHours +
          ' hours) for stream ' + data.StreamName +
          ' can not be shorter than existing retention period (' + stream.RetentionPeriodHours +
          ' hours). Use DecreaseRetentionPeriod API.'))
      }

      stream.RetentionPeriodHours = data.RetentionPeriodHours

      metaDb.put(data.StreamName, stream, function(err) {
        if (err) return cb(err)

        cb()
      })
    })
  })
}
