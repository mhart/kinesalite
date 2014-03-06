
module.exports = function getRecords(store, data, cb) {

  var err = new Error
  err.statusCode = 400
  err.body = {
    __type: 'InvalidArgumentException',
    message: 'Invalid ShardIterator.',
  }
  return cb(err)

}

