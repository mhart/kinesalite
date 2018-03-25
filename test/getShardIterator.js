var async = require('async'),
    BigNumber = require('bignumber.js'),
    helpers = require('./helpers')

var target = 'GetShardIterator',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)
    assertInternalFailure = helpers.assertInternalFailure.bind(null, target)

describe('getShardIterator', function() {

  describe('serializations', function() {

    it('should return SerializationException when ShardId is not an String', function(done) {
      assertType('ShardId', 'String', done)
    })

    it('should return SerializationException when ShardIteratorType is not a String', function(done) {
      assertType('ShardIteratorType', 'String', done)
    })

    it('should return SerializationException when StartingSequenceNumber is not a String', function(done) {
      assertType('StartingSequenceNumber', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })
  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({}, [
        'Value null at \'shardId\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'shardIteratorType\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', ShardIteratorType: '', ShardId: '', StartingSequenceNumber: ''}, [
        'Value \'\' at \'shardId\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'shardId\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'shardIteratorType\' failed to satisfy constraint: ' +
        'Member must satisfy enum value set: [AFTER_SEQUENCE_NUMBER, LATEST, AT_TIMESTAMP, AT_SEQUENCE_NUMBER, TRIM_HORIZON]',
        'Value \'\' at \'startingSequenceNumber\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,128})',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, ShardId: name, ShardIteratorType: 'LATEST'}, [
        'Value \'' + name + '\' at \'shardId\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, ShardId: name, ShardIteratorType: 'LATEST'}, [
        'Value \'' + name + '\' at \'shardId\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return ResourceNotFoundException if unknown stream and shard ID just small enough', function(done) {
      var name1 = randomName(), name2 = '2147483647'
      assertNotFound({StreamName: name1, ShardId: name2, ShardIteratorType: 'LATEST'},
        'Shard shardId-002147483647 in stream ' + name1 + ' under account ' +
          helpers.awsAccountId + ' does not exist', done)
    })

    it('should return ResourceNotFoundException if unknown stream and random shard name', function(done) {
      var name1 = randomName(), name2 = randomName() + '-2147483647'
      assertNotFound({StreamName: name1, ShardId: name2, ShardIteratorType: 'LATEST'},
        'Shard shardId-002147483647 in stream ' + name1 + ' under account ' +
          helpers.awsAccountId + ' does not exist', done)
    })

    it('should return ResourceNotFoundException if unknown stream and short prefix', function(done) {
      var name1 = randomName(), name2 = 'a-00002147483647'
      assertNotFound({StreamName: name1, ShardId: name2, ShardIteratorType: 'LATEST'},
        'Shard shardId-002147483647 in stream ' + name1 + ' under account ' +
          helpers.awsAccountId + ' does not exist', done)
    })

    it('should return ResourceNotFoundException if unknown stream and no prefix', function(done) {
      var name1 = randomName(), name2 = '-00002147483647'
      assertNotFound({StreamName: name1, ShardId: name2, ShardIteratorType: 'LATEST'},
        'Shard shardId-002147483647 in stream ' + name1 + ' under account ' +
          helpers.awsAccountId + ' does not exist', done)
    })

    it('should return ResourceNotFoundException if unknown stream and shard ID too big', function(done) {
      var name1 = randomName(), name2 = '2147483648'
      assertNotFound({StreamName: name1, ShardId: name2, ShardIteratorType: 'LATEST'},
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and raw shard ID too big', function(done) {
      var name1 = randomName(), name2 = 'shardId-002147483648'
      assertNotFound({StreamName: name1, ShardId: name2, ShardIteratorType: 'LATEST'},
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and string shard ID', function(done) {
      var name1 = randomName(), name2 = 'ABKLFD8'
      assertNotFound({StreamName: name1, ShardId: name2, ShardIteratorType: 'LATEST'},
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.', done)
    })

    it('should return ResourceNotFoundException if unknown stream and exponent shard ID', function(done) {
      var name1 = randomName(), name2 = '2.14E4'
      assertNotFound({StreamName: name1, ShardId: name2, ShardIteratorType: 'LATEST'},
        'Could not find shard ' + name2 + ' in stream ' + name1 + ' under account ' + helpers.awsAccountId + '.', done)
    })

    it('should return ResourceNotFoundException if known stream and raw shard ID does not exist', function(done) {
      var name1 = helpers.testStream, name2 = 'shardId-5'
      assertNotFound({StreamName: name1, ShardId: name2, ShardIteratorType: 'AT_SEQUENCE_NUMBER', StartingSequenceNumber: '5'},
        'Shard shardId-000000000005 in stream ' + name1 + ' under account ' +
          helpers.awsAccountId + ' does not exist', done)
    })

    it('should return InvalidArgumentException if AT_SEQUENCE_NUMBER and no StartingSequenceNumber', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, ShardId: 'shardId-0', ShardIteratorType: 'AT_SEQUENCE_NUMBER'},
        'Must either specify (1) AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER and StartingSequenceNumber or ' +
        '(2) TRIM_HORIZON or LATEST and no StartingSequenceNumber. ' +
        'Request specified AT_SEQUENCE_NUMBER and no StartingSequenceNumber.', done)
    })

    it('should return InvalidArgumentException if AFTER_SEQUENCE_NUMBER and no StartingSequenceNumber', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, ShardId: 'shardId-0', ShardIteratorType: 'AFTER_SEQUENCE_NUMBER'},
        'Must either specify (1) AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER and StartingSequenceNumber or ' +
        '(2) TRIM_HORIZON or LATEST and no StartingSequenceNumber. ' +
        'Request specified AFTER_SEQUENCE_NUMBER and no StartingSequenceNumber.', done)
    })

    it('should return InvalidArgumentException if LATEST and StartingSequenceNumber', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, ShardId: 'shardId-0', ShardIteratorType: 'LATEST', StartingSequenceNumber: '5'},
        'Must either specify (1) AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER and StartingSequenceNumber or ' +
        '(2) TRIM_HORIZON or LATEST and no StartingSequenceNumber. ' +
        'Request specified LATEST and also a StartingSequenceNumber.', done)
    })

    it('should return InvalidArgumentException if TRIM_HORIZON and StartingSequenceNumber', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, ShardId: 'shardId-0', ShardIteratorType: 'TRIM_HORIZON', StartingSequenceNumber: '5'},
        'Must either specify (1) AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER and StartingSequenceNumber or ' +
        '(2) TRIM_HORIZON or LATEST and no StartingSequenceNumber. ' +
        'Request specified TRIM_HORIZON and also a StartingSequenceNumber.', done)
    })

    it('should return InvalidArgumentException if shard mismatches simple sequence', function(done) {
      var name1 = helpers.testStream, name2 = 'shardId-0'
      assertInvalidArgument({StreamName: name1, ShardId: name2, ShardIteratorType: 'AT_SEQUENCE_NUMBER', StartingSequenceNumber: '5'},
        'Invalid StartingSequenceNumber. It encodes shardId-000000000005, ' +
          'while it was used in a call to a shard with shardId-000000000000', done)
    })

    it('should return InternalFailure if using small (old?) sequence number', function(done) {
      var name1 = helpers.testStream, name2 = 'shardId-0'
      assertInternalFailure({StreamName: name1, ShardId: name2, ShardIteratorType: 'AT_SEQUENCE_NUMBER', StartingSequenceNumber: '0'}, done)
    })

    it('should return InvalidArgumentException if using sequence number with large date', function(done) {
      var name1 = helpers.testStream, name2 = 'shardId-0', seq = new BigNumber('13bb2cc3d80000000000000000000000', 16).toFixed()
      assertInvalidArgument({StreamName: name1, ShardId: name2, ShardIteratorType: 'AT_SEQUENCE_NUMBER', StartingSequenceNumber: seq},
        'StartingSequenceNumber 26227199374822427428162556223570313216 used in GetShardIterator on shard ' +
        'shardId-000000000000 in stream ' + name1 + ' under account ' + helpers.awsAccountId +
        ' is invalid.', done)
    })

    it('should return InvalidArgumentException for 8 in index in StartingSequenceNumber', function(done) {
      var seq = new BigNumber('20000000000800000000000000000000000000000000002', 16).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, ShardId: 'shardId-0', ShardIteratorType: 'AT_SEQUENCE_NUMBER', StartingSequenceNumber: seq},
        'StartingSequenceNumber ' + seq + ' used in GetShardIterator on shard ' +
        'shardId-000000000000 in stream ' + helpers.testStream + ' under account ' + helpers.awsAccountId +
        ' is invalid.', done)
    })

    it('should return InvalidArgumentException for 8 near end of StartingSequenceNumber', function(done) {
      var seq = new BigNumber('20000000000000000000000000000000000000800000002', 16).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, ShardId: 'shardId-0', ShardIteratorType: 'AT_SEQUENCE_NUMBER', StartingSequenceNumber: seq},
        'Invalid StartingSequenceNumber. It encodes shardId--02147483648, ' +
        'while it was used in a call to a shard with shardId-000000000000', done)
    })

    it('should return InvalidArgumentException if AT_TIMESTAMP and no Timestamp', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, ShardId: 'shardId-0', ShardIteratorType: 'AT_TIMESTAMP'},
        'Must specify timestampInMillis parameter for iterator of type AT_TIMESTAMP. Current request has no timestamp parameter.', done)
    })
  })

  describe('functionality', function() {

    it('should work with random shard ID with hyphen', function(done) {

      function testType(shardIteratorType, cb) {
        request(opts({
          StreamName: helpers.testStream,
          ShardId: randomName() + '-0',
          ShardIteratorType: shardIteratorType,
        }), function(err, res) {
          if (err) return cb(err)
          res.statusCode.should.equal(200)
          Object.keys(res.body).should.eql(['ShardIterator'])
          helpers.assertShardIterator(res.body.ShardIterator, helpers.testStream)
          cb()
        })
      }

      async.forEach(['LATEST', 'TRIM_HORIZON'], testType, done)
    })

    it('should work with different length stream names', function(done) {
      this.timeout(100000)

      var stream1 = (randomName() + new Array(14).join('0')).slice(0, 29),
        stream2 = (randomName() + new Array(14).join('0')).slice(0, 30)

      request(helpers.opts('CreateStream', {StreamName: stream1, ShardCount: 1}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(helpers.opts('CreateStream', {StreamName: stream2, ShardCount: 1}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          helpers.waitUntilActive(stream1, function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            request(opts({
              StreamName: stream1,
              ShardId: 'shard-0',
              ShardIteratorType: 'LATEST',
            }), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              Object.keys(res.body).should.eql(['ShardIterator'])
              helpers.assertShardIterator(res.body.ShardIterator, stream1)

              helpers.waitUntilActive(stream2, function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                request(opts({
                  StreamName: stream2,
                  ShardId: 'shard-0',
                  ShardIteratorType: 'LATEST',
                }), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)

                  Object.keys(res.body).should.eql(['ShardIterator'])
                  helpers.assertShardIterator(res.body.ShardIterator, stream2)

                  request(helpers.opts('DeleteStream', {StreamName: stream1}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)

                    request(helpers.opts('DeleteStream', {StreamName: stream2}), done)
                  })
                })
              })
            })
          })
        })
      })
    })

  })
})
