var https = require('https'),
    aws4 = require('aws4'),
    async = require('async'),
    once = require('once'),
    BigNumber = require('bignumber.js'),
    kinesalite = require('..')

var port = 10000 + Math.round(Math.random() * 10000),
    requestOpts = process.env.REMOTE ?
      {host: 'kinesis.us-east-1.amazonaws.com', method: 'POST'} :
      {host: 'localhost', port: port, method: 'POST', rejectUnauthorized: false}

https.globalAgent.maxSockets = Infinity

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
exports.assertSequenceNumber = assertSequenceNumber
exports.randomString = randomString
exports.randomName = randomName
exports.waitUntilActive = waitUntilActive
exports.waitUntilDeleted = waitUntilDeleted
exports.testStream = randomName()
// For testing:
//exports.testStream = '__kinesalite_test_1'

var kinesaliteServer = kinesalite({path: process.env.KINESALITE_PATH})

before(function(done) {
  this.timeout(200000)
  kinesaliteServer.listen(port, function(err) {
    if (err) return done(err)
    createTestStreams(done)
    //done()
  })
  // TODO: Resolve account ID - can be found from a DeleteStream request with bogus name
  exports.awsAccountId = (process.env.AWS_ACCOUNT_ID || '0000-0000-0000').replace(/[^\d]/g, '')
})

after(function(done) {
  this.timeout(200000)
  deleteTestStreams(function(err) {
    if (err) return done(err)
    kinesaliteServer.close(done)
  })
})

function request(opts, cb) {
  if (typeof opts === 'function') { cb = opts; opts = {} }
  cb = once(cb)
  for (var key in requestOpts) {
    if (opts[key] === undefined)
      opts[key] = requestOpts[key]
  }
  if (!opts.noSign) {
    aws4.sign(opts)
    opts.noSign = true // don't sign twice if calling recursively
  }
  //console.log(opts)
  https.request(opts, function(res) {
    res.setEncoding('utf8')
    res.on('error', cb)
    res.body = ''
    res.on('data', function(chunk) { res.body += chunk })
    res.on('end', function() {
      try { res.body = JSON.parse(res.body) } catch (e) {}
      cb(null, res)
    })
  }).on('error', cb).end(opts.body)
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
  switch(type) {
    case 'Boolean':
      msgs = [
        [23, 'class java.lang.Short can not be converted to an Boolean'],
        [-2147483648, 'class java.lang.Integer can not be converted to an Boolean'],
        [2147483648, 'class java.lang.Long can not be converted to an Boolean'],
        // For some reason, doubles are fine
        //[34.56, 'class java.lang.Double can not be converted to an Boolean'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
      ]
      break
    case 'String':
      msgs = [
        [true, 'class java.lang.Boolean can not be converted to an String'],
        [23, 'class java.lang.Short can not be converted to an String'],
        [-2147483648, 'class java.lang.Integer can not be converted to an String'],
        [2147483648, 'class java.lang.Long can not be converted to an String'],
        [34.56, 'class java.lang.Double can not be converted to an String'],
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
        [23, 'class java.lang.Short can not be converted to a Blob'],
        [-2147483648, 'class java.lang.Integer can not be converted to a Blob'],
        [2147483648, 'class java.lang.Long can not be converted to a Blob'],
        [34.56, 'class java.lang.Double can not be converted to a Blob'],
        [[], 'Start of list found where not expected'],
        [{}, 'Start of structure or map found where not expected.'],
        ['23456', '\'23456\' can not be converted to a Blob: Base64 encoded length is expected a multiple of 4 bytes but found: 5'],
        ['=+/=', '\'=+/=\' can not be converted to a Blob: Invalid Base64 character: \'=\''],
        ['+/+=', '\'+/+=\' can not be converted to a Blob: Invalid last non-pad Base64 character dectected'],
      ]
      break
    case 'List':
      msgs = [
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
    if (msg instanceof RegExp) {
      res.body.__type.should.equal('ValidationException')
      res.body.message.should.match(msg)
    } else {
      res.body.should.eql({
        __type: 'ValidationException',
        message: msg,
      })
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

function assertSequenceNumber(seqNum, shardIx, timestamp) {
  var hex = BigNumber(seqNum).toString(16)
  hex.should.match(new RegExp('^20[0-9a-f]{8}' + shardIx + '[0-9a-f]{16}000[0-9a-f]{8}0000000' + shardIx + '2$'))
  parseInt(hex.slice(2, 10), 16).should.be.within(new Date('2015-01-01') / 1000, Date.now() / 1000 - 2)
  parseInt(hex.slice(11, 27), 16).should.be.greaterThan(-1)
  parseInt(hex.slice(30, 38), 16).should.be.within(Math.floor(timestamp / 1000) - 3, Date.now() / 1000)
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

