var http = require('http'),
    https = require('https'),
    aws4 = require('aws4'),
    async = require('async'),
    once = require('once'),
    BigNumber = require('bignumber.js'),
    kinesalite = require('..')

http.globalAgent.maxSockets = https.globalAgent.maxSockets = Infinity

exports.awsRegion = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || 'us-east-1'
exports.awsAccountId = (process.env.AWS_ACCOUNT_ID || '0000-0000-0000').replace(/[^\d]/g, '') // also resolved below
exports.shardLimit = 50 // also resolved below
exports.version = 'Kinesis_20131202'
exports.prefix = '__kinesalite_test_'
exports.request = request
exports.opts = opts
exports.assertSerialization = assertSerialization
exports.assertType = assertType
exports.assertValidation = assertValidation
exports.assertNotFound = assertNotFound
exports.assertInUse = assertInUse
exports.assertLimitExceeded = assertLimitExceeded
exports.assertInvalidArgument = assertInvalidArgument
exports.assertInternalFailure = assertInternalFailure
exports.assertSequenceNumber = assertSequenceNumber
exports.assertShardIterator = assertShardIterator
exports.assertArrivalTimes = assertArrivalTimes
exports.randomString = randomString
exports.randomName = randomName
exports.waitUntilActive = waitUntilActive
exports.waitUntilDeleted = waitUntilDeleted
exports.testStream = randomName()
// For testing:
// exports.testStream = '__kinesalite_test_1'

var port = 10000 + Math.round(Math.random() * 10000),
    requestOpts = process.env.REMOTE ?
      {host: 'kinesis.' + exports.awsRegion + '.amazonaws.com', method: 'POST', ssl: true} :
      {host: '127.0.0.1', port: port, method: 'POST'}

var kinesaliteServer = kinesalite({path: process.env.KINESALITE_PATH})

before(function(done) {
  this.timeout(200000)
  kinesaliteServer.listen(port, function(err) {
    if (err) return done(err)
    async.parallel([
      resolveAccountId,
      resolveShardLimit,
      createTestStreams,
    ], done)
  })
})

after(function(done) {
  this.timeout(200000)
  deleteTestStreams(function(err) {
    if (err) return done(err)
    kinesaliteServer.close(done)
  })
})

function request(options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  cb = once(cb)
  for (var key in requestOpts) {
    if (options[key] === undefined)
      options[key] = requestOpts[key]
  }
  if (!options.noSign) {
    aws4.sign(options)
    options.noSign = true // don't sign twice if calling recursively
  }
  // console.log(options)
  (options.ssl ? https : http).request(options, function(res) {
    res.setEncoding('utf8')
    res.on('error', cb)
    res.body = ''
    res.on('data', function(chunk) { res.body += chunk })
    res.on('end', function() {
      try { res.body = JSON.parse(res.body) } catch (e) {} // eslint-disable-line no-empty
      if (res.body.__type == 'LimitExceededException' && /^Rate exceeded/.test(res.body.message))
        return setTimeout(request, Math.floor(Math.random() * 1000), options, cb)
      cb(null, res)
    })
  }).on('error', cb).end(options.body)
}

function opts(target, data) {
  return {
    headers: {
      'Content-Type': 'application/x-amz-json-1.1',
      'X-Amz-Target': exports.version + '.' + target,
    },
    body: JSON.stringify(data),
  }
}

function randomString() {
  return String(Math.random() * 0x100000000)
}

function randomName() {
  return exports.prefix + randomString()
}

function assertSerialization(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'SerializationException',
      Message: msg,
    })
    done()
  })
}

function assertType(target, property, type, done) {
  var msgs = [], pieces = property.split('.')
  switch (type) {
    case 'Boolean':
      msgs = [
        ['23', '\'23\' can not be converted to an Boolean'],
        [23, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an Boolean'],
        [-2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an Boolean'],
        [2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an Boolean'],
        // For some reason, doubles are fine
        // [34.56, 'class java.lang.Double can not be converted to an Boolean'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'String':
      msgs = [
        [true, 'class java.lang.Boolean can not be converted to an String'],
        [23, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String'],
        [-2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String'],
        [2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String'],
        [34.56, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'Integer':
      msgs = [
        ['23', 'class java.lang.String can not be converted to an Integer'],
        [true, 'class java.lang.Boolean can not be converted to an Integer'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'Long':
      msgs = [
        ['23', 'class java.lang.String can not be converted to an Long'],
        [true, 'class java.lang.Boolean can not be converted to an Long'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'Blob':
      msgs = [
        [true, 'class java.lang.Boolean can not be converted to a Blob'],
        [23, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob'],
        [-2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob'],
        [2147483648, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob'],
        [34.56, 'class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
        ['23456', '\'23456\' can not be converted to a Blob: Base64 encoded length is expected a multiple of 4 bytes but found: 5'],
        ['=+/=', '\'=+/=\' can not be converted to a Blob: Invalid Base64 character: \'=\''],
        ['+/+=', '\'+/+=\' can not be converted to a Blob: Invalid last non-pad Base64 character dectected'],
      ]
      break
    case 'List':
      msgs = [
        ['23', 'Expected list or null'],
        [true, 'Expected list or null'],
        [23, 'Expected list or null'],
        [-2147483648, 'Expected list or null'],
        [2147483648, 'Expected list or null'],
        [34.56, 'Expected list or null'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'Map':
      msgs = [
        ['23', 'Expected map or null'],
        [true, 'Expected map or null'],
        [23, 'Expected map or null'],
        [-2147483648, 'Expected map or null'],
        [2147483648, 'Expected map or null'],
        [34.56, 'Expected map or null'],
        [[], 'Start of list found where not expected'],
      ]
      break
    case 'Structure':
      msgs = [
        ['23', 'Expected null'],
        [true, 'Expected null'],
        [23, 'Expected null'],
        [-2147483648, 'Expected null'],
        [2147483648, 'Expected null'],
        [34.56, 'Expected null'],
        [[], 'Start of list found where not expected'],
      ]
      break
    default:
      throw new Error('Unknown type: ' + type)
  }
  async.forEach(msgs, function(msg, cb) {
    var data = {}, child = data, i, ix
    for (i = 0; i < pieces.length - 1; i++) {
      ix = Array.isArray(child) ? 0 : pieces[i]
      child = child[ix] = pieces[i + 1] === '0' ? [] : {}
    }
    ix = Array.isArray(child) ? 0 : pieces[pieces.length - 1]
    child[ix] = msg[0]
    assertSerialization(target, data, msg[1], cb)
  }, done)
}

function assertValidation(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.__type.should.equal('ValidationException')
    var resMsg = res.body.message
    if (Array.isArray(msg)) {
      var msgPrefix = msg.length + ' validation ' + (msg.length == 1 ? 'error' : 'errors') + ' detected: '
      resMsg.should.startWith(msgPrefix)
      resMsg = resMsg.slice(msgPrefix.length)
      while (msg.length) {
        for (var i = 0; i < msg.length; i++) {
          if (resMsg.indexOf(msg[i]) === 0) {
            resMsg = resMsg.slice(msg[i].length)
            if (resMsg.indexOf('; ') === 0) resMsg = resMsg.slice(2)
            break
          }
        }
        if (i >= msg.length) {
          throw new Error('Could not match ' + resMsg + ' from ' + msg)
        }
        msg.splice(i, 1)
      }
    } else if (msg instanceof RegExp) {
      res.body.message.should.match(msg)
    } else {
      res.body.message.should.equal(msg)
    }
    done()
  })
}

function assertNotFound(target, data, msg, done) {
  if (!Array.isArray(msg)) msg = [msg]
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.__type.should.equal('ResourceNotFoundException')
    msg.should.containEql(res.body.message)
    done()
  })
}

function assertInUse(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'ResourceInUseException',
      message: msg,
    })
    done()
  })
}

function assertLimitExceeded(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'LimitExceededException',
      message: msg,
    })
    done()
  })
}

function assertInvalidArgument(target, data, msg, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(400)
    res.body.should.eql({
      __type: 'InvalidArgumentException',
      message: msg,
    })
    done()
  })
}

function assertInternalFailure(target, data, done) {
  request(opts(target, data), function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(500)
    res.body.should.eql({
      __type: 'InternalFailure',
    })
    done()
  })
}

function assertSequenceNumber(seqNum, shardIx, timestamp) {
  var hex = new BigNumber(seqNum).toString(16)
  hex.should.match(new RegExp('^20[0-9a-f]{8}' + shardIx + '[0-9a-f]{16}000[0-9a-f]{8}0000000' + shardIx + '2$'))
  parseInt(hex.slice(2, 10), 16).should.be.within(new Date('2015-01-01') / 1000, Date.now() / 1000 + 1)
  parseInt(hex.slice(11, 27), 16).should.be.above(-1)
  parseInt(hex.slice(30, 38), 16).should.be.within(Math.floor(timestamp / 1000) - 4, Date.now() / 1000 + 1)
}

function assertShardIterator(shardIterator, streamName) {
  var buffer = new Buffer(shardIterator, 'base64')
  shardIterator.should.equal(buffer.toString('base64'))
  // XXX: Length checks seem to be a bit unreliable
  try {
    buffer.should.have.length(152 + (Math.floor((streamName.length + 2) / 16) * 16))
  } catch (e) {
    buffer.should.have.length(152 + (Math.floor((streamName.length + 14) / 16) * 16))
  }
  buffer.slice(0, 8).toString('hex').should.equal('0000000000000001')
}

function assertArrivalTimes(records) {
  records.should.not.be.empty()
  records[0].should.have.property('ApproximateArrivalTimestamp')
  var arrivalTime = records[0].ApproximateArrivalTimestamp
  arrivalTime.should.be.within(1000000000, 10000000000)
  records.forEach(function(record) {
    var diff = Math.abs(arrivalTime - record.ApproximateArrivalTimestamp)
    diff.should.be.below(1)
    var seqTime = parseInt(new BigNumber(record.SequenceNumber).toString(16).slice(30, 38), 16)
    diff = record.ApproximateArrivalTimestamp - seqTime
    diff.should.be.within(0, 3)
  })
}

function createTestStreams(done) {
  var streams = [{
    StreamName: exports.testStream,
    ShardCount: 3,
  }]
  async.forEach(streams, createAndWait, done)
}

function deleteTestStreams(done) {
  request(opts('ListStreams', {}), function(err, res) {
    if (err) return done(err)
    var names = res.body.StreamNames.filter(function(name) { return name.indexOf(exports.prefix) === 0 })
    async.forEach(names, deleteAndWait, done)
  })
}

function createAndWait(stream, done) {
  request(opts('CreateStream', stream), function(err, res) {
    if (err) return done(err)
    if (res.body.__type)
      return done(new Error(res.body.__type + ': ' + res.body.message))
    setTimeout(waitUntilActive, 1000, stream.StreamName, done)
  })
}

function deleteAndWait(name, done) {
  request(opts('DeleteStream', {StreamName: name}), function(err, res) {
    if (err) return done(err)
    if (res.body.__type == 'ResourceInUseException')
      return setTimeout(deleteAndWait, 1000, name, done)
    else if (res.body.__type)
      return done(new Error(res.body.__type + ': ' + res.body.message))
    setTimeout(waitUntilDeleted, 1000, name, done)
  })
}

function waitUntilActive(name, done) {
  request(opts('DescribeStream', {StreamName: name}), function(err, res) {
    if (err) return done(err)
    if (res.body.__type)
      return done(new Error(res.body.__type + ': ' + res.body.message))
    else if (res.body.StreamDescription.StreamStatus == 'ACTIVE')
      return done(null, res)
    setTimeout(waitUntilActive, 1000, name, done)
  })
}

function waitUntilDeleted(name, done) {
  request(opts('DescribeStream', {StreamName: name}), function(err, res) {
    if (err) return done(err)
    if (res.body.__type == 'ResourceNotFoundException')
      return done(null, res)
    else if (res.body.__type)
      return done(new Error(res.body.__type + ': ' + res.body.message))
    setTimeout(waitUntilDeleted, 1000, name, done)
  })
}

function resolveAccountId(done) {
  request(opts('DeleteStream', {StreamName: randomName()}), function(err, res) {
    if (err) return done(err)
    if (res.statusCode == 400 && res.body.__type == 'ResourceNotFoundException') {
      var match = res.body.message.match(/account (.+) not found/)
      if (match) exports.awsAccountId = match[1]
    }
    done()
  })
}

function resolveShardLimit(done) {
  request(opts('CreateStream', {StreamName: randomName(), ShardCount: 100000}), function(err, res) {
    if (err) return done(err)
    if (res.statusCode == 400 && res.body.__type == 'LimitExceededException') {
      var match = res.body.message.match(/Limit: (\d+). Number of additional/)
      if (match) exports.shardLimit = +match[1]
    } else if (res.statusCode == 200) {
      throw new Error('WHOOPS, JUST CREATED A HUGE SHARDED STREAM, DELETE DELETE')
    }
    done()
  })
}
