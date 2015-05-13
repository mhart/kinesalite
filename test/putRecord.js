var BigNumber = require('bignumber.js'),
    helpers = require('./helpers')

var target = 'PutRecord',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInternalFailure = helpers.assertInternalFailure.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('putRecord ', function() {

  describe('serializations', function() {

    it('should return SerializationException when Data is not a Blob', function(done) {
      assertType('Data', 'Blob', done)
    })

    it('should return SerializationException when ExplicitHashKey is not a String', function(done) {
      assertType('ExplicitHashKey', 'String', done)
    })

    it('should return SerializationException when PartitionKey is not a String', function(done) {
      assertType('PartitionKey', 'String', done)
    })

    it('should return SerializationException when SequenceNumberForOrdering is not a String', function(done) {
      assertType('SequenceNumberForOrdering', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({},
        '3 validation errors detected: ' +
        'Value null at \'partitionKey\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'data\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', PartitionKey: '', Data: '', ExplicitHashKey: '', SequenceNumberForOrdering: ''},
        '5 validation errors detected: ' +
        'Value \'\' at \'sequenceNumberForOrdering\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,128}); ' +
        'Value \'\' at \'partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'explicitHashKey\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,38}); ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a'), name2 = new Array(257 + 1).join('a'),
        data = new Buffer(51201).toString('base64')
      assertValidation({StreamName: name, PartitionKey: name2, Data: data, ExplicitHashKey: ''},
        '4 validation errors detected: ' +
        'Value \'' + name2 + '\' at \'partitionKey\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 256; ' +
        'Value \'\' at \'explicitHashKey\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,38}); ' +
        'Value \'java.nio.HeapByteBuffer[pos=0 lim=51201 cap=51201]\' at \'data\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 51200; ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = randomName(), name2 = helpers.randomString()
      assertNotFound({StreamName: name1, PartitionKey: name2, Data: ''},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

    it('should return InvalidArgumentException for out of bounds ExplicitHashKey', function(done) {
      var hashKey = new BigNumber(2).pow(128).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', ExplicitHashKey: hashKey},
        'Invalid ExplicitHashKey. ExplicitHashKey must be in the range: [0, 2^128-1]. Specified value was ' + hashKey, done)
    })

    it('should return InvalidArgumentException for version 0 in SequenceNumberForOrdering', function(done) {
      var seq = new BigNumber('20000000000000000000000000000000000000000000000', 16).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq},
        'ExclusiveMinimumSequenceNumber ' + seq + ' used in PutRecord on stream ' + helpers.testStream +
        ' under account ' + helpers.awsAccountId + ' is invalid.', done)
    })

    it('should return InvalidArgumentException for version 4 in SequenceNumberForOrdering', function(done) {
      var seq = new BigNumber('20000000000000000000000000000000000000000000004', 16).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq},
        'ExclusiveMinimumSequenceNumber ' + seq + ' used in PutRecord on stream ' + helpers.testStream +
        ' under account ' + helpers.awsAccountId + ' is invalid.', done)
    })

    it('should return InternalFailure for version 3 in SequenceNumberForOrdering', function(done) {
      var seq = new BigNumber('20000000000000000000000000000000000000000000003', 16).toFixed()
      assertInternalFailure({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq}, done)
    })

    it('should return InvalidArgumentException for initial 3 in SequenceNumberForOrdering', function(done) {
      var seq = new BigNumber('30000000000000000000000000000000000000000000001', 16).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq},
        'ExclusiveMinimumSequenceNumber ' + seq + ' used in PutRecord on stream ' + helpers.testStream +
        ' under account ' + helpers.awsAccountId + ' is invalid.', done)
    })

    it('should return InvalidArgumentException for initial 1 in SequenceNumberForOrdering', function(done) {
      var seq = new BigNumber('1ffffffffff7fffffffffffffff000000000007fffffff2', 16).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq},
        'ExclusiveMinimumSequenceNumber ' + seq + ' used in PutRecord on stream ' + helpers.testStream +
        ' under account ' + helpers.awsAccountId + ' is invalid.', done)
    })

    it('should return InvalidArgumentException for 8 in index in SequenceNumberForOrdering', function(done) {
      var seq = new BigNumber('20000000000800000000000000000000000000000000002', 16).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq},
        'ExclusiveMinimumSequenceNumber ' + seq + ' used in PutRecord on stream ' + helpers.testStream +
        ' under account ' + helpers.awsAccountId + ' is invalid.', done)
    })

    // Not really sure that this is necessary - seems obscure
    it.skip('should return InternalFailure for 8 and real time in SequenceNumberForOrdering', function(done) {
      var seq = new BigNumber('2ffffffffff7fffffffffffffff000' + Math.floor(Date.now() / 1000).toString(16) + '7fffffff2', 16).toFixed()
      assertInternalFailure({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq}, done)
    })

    it('should return InvalidArgumentException for future time in SequenceNumberForOrdering', function(done) {
      var seq = new BigNumber('200000000000000000000000000000' + Math.floor(Date.now() / 1000 + 2).toString(16) + '000000002', 16).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq},
        'ExclusiveMinimumSequenceNumber ' + seq + ' used in PutRecord on stream ' + helpers.testStream +
        ' under account ' + helpers.awsAccountId + ' is invalid.', done)
    })

    it('should return InvalidArgumentException if using sequence number with large date', function(done) {
      var seq = new BigNumber('13bb2cc3d80000000000000000000000', 16).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq},
        'ExclusiveMinimumSequenceNumber 26227199374822427428162556223570313216 used in PutRecord on stream ' +
        helpers.testStream + ' under account ' + helpers.awsAccountId + ' is invalid.', done)
    })

  })

  describe('functionality', function() {

    it('should work with empty Data', function(done) {
      var now = Date.now()
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: ''}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 0, now)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000000'})
        done()
      })
    })

    it('should work with large Data', function(done) {
      var now = Date.now(), data = new Buffer(51200).toString('base64')
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: data}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 0, now)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000000'})
        done()
      })
    })

    it('should work with final ExplicitHashKey', function(done) {
      var hashKey = new BigNumber(2).pow(128).minus(1).toFixed(), now = Date.now()
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', ExplicitHashKey: hashKey}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 2, now)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000002'})
        done()
      })
    })

    it('should work with ExplicitHashKey just below range', function(done) {
      var hashKey = new BigNumber(2).pow(128).div(3).floor().times(2).minus(1).toString(10), now = Date.now()
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', ExplicitHashKey: hashKey}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 1, now)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000001'})
        done()
      })
    })

    it('should work with ExplicitHashKey just above range', function(done) {
      var hashKey = new BigNumber(2).pow(128).div(3).floor().times(2).toString(10), now = Date.now()
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', ExplicitHashKey: hashKey}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 2, now)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000002'})
        done()
      })
    })

    it('should work with SequenceNumberForOrdering all f', function(done) {
      var seq = new BigNumber('2ffffffffff7fffffffffffffff000000000007fffffff2', 16).toFixed(), now = Date.now()
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 0, now)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000000'})
        done()
      })
    })

    it('should work with SequenceNumberForOrdering all 0', function(done) {
      var seq = new BigNumber('20000000000000000000000000000000000000000000002', 16).toFixed(), now = Date.now()
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 0, now)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000000'})
        done()
      })
    })

    it('should work with SequenceNumberForOrdering all 0 with version 1', function(done) {
      var seq = new BigNumber('20000000000000000000000000000000000000000000001', 16).toFixed(), now = Date.now()
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 0, now)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000000'})
        done()
      })
    })

    it('should work with SequenceNumberForOrdering if time is now', function(done) {
      var seq = new BigNumber('2ffffffffff7fffffff7fffffff000' + Math.floor(Date.now() / 1000 - 2).toString(16) +
        '7fffffff2', 16).toFixed(), now = Date.now()
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 0, now)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000000'})
        done()
      })
    })

    it('should work with SequenceNumberForOrdering if 8 near end', function(done) {
      var seq = new BigNumber('20000000000000000000000000000000000000800000002', 16).toFixed()
      request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: '', SequenceNumberForOrdering: seq}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        helpers.assertSequenceNumber(res.body.SequenceNumber, 0, 0)
        delete res.body.SequenceNumber
        res.body.should.eql({ShardId: 'shardId-000000000000'})
        done()
      })
    })

    it('should safely put concurrent, sequential records', function(done) {

      var remaining = 100, seqIxs = []

      function putRecords() {
        var now = Date.now()
        request(opts({StreamName: helpers.testStream, PartitionKey: 'a', Data: ''}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          seqIxs.push(parseInt(new BigNumber(res.body.SequenceNumber).toString(16).slice(11, 27), 16))

          helpers.assertSequenceNumber(res.body.SequenceNumber, 0, now)
          delete res.body.SequenceNumber
          res.body.should.eql({ShardId: 'shardId-000000000000'})

          now = Date.now()
          request(opts({StreamName: helpers.testStream, PartitionKey: 'b', Data: ''}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            seqIxs.push(parseInt(new BigNumber(res.body.SequenceNumber).toString(16).slice(11, 27), 16))

            helpers.assertSequenceNumber(res.body.SequenceNumber, 1, now)
            delete res.body.SequenceNumber
            res.body.should.eql({ShardId: 'shardId-000000000001'})

            if (!--remaining) checkIxs()
          })
        })
      }

      function checkIxs() {
        seqIxs.sort(function(a, b) { return a - b })
        for (var i = 1; i < seqIxs.length; i++) {
          var diff = seqIxs[i] - seqIxs[i - 1]
          diff.should.equal(1)
        }
        done()
      }

      for (var i = 0; i < remaining; i++) {
        putRecords()
      }
    })

  })

})
