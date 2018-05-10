var helpers = require('./helpers')

var target = 'ListShards',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('listShards', function() {

  describe('serializations', function() {

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

    it('should return SerializationException when MaxResults is not an Integer', function(done) {
      assertType('MaxResults', 'Integer', done)
    })

    it('should return SerializationException when ExclusiveStartShardId is not a String', function(done) {
      assertType('ExclusiveStartShardId', 'String', done)
    })

    it('should return SerializationException when NextToken is not a String', function(done) {
      assertType('NextToken', 'String', done)
    })

    it('should return SerializationException when StreamCreationTimestamp is not a Timestamp', function(done) {
      assertType('StreamCreationTimestamp', 'Timestamp', done)
    })

  })

  describe('validations', function() {

    it('should return InvalidArgumentException for no StreamName or NextToken', function(done) {
      assertInvalidArgument({}, 'Either NextToken or StreamName should be provided.', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', NextToken: '', MaxResults: 0, ExclusiveStartShardId: ''}, [
        'Value \'0\' at \'maxResults\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1',
        'Value \'\' at \'exclusiveStartShardId\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'exclusiveStartShardId\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'nextToken\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, MaxResults: 100000, ExclusiveStartShardId: name}, [
        'Value \'100000\' at \'maxResults\' failed to satisfy constraint: ' +
        'Member must have value less than or equal to 10000',
        'Value \'' + name + '\' at \'exclusiveStartShardId\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return InvalidArgumentException if both StreamName and NextToken', function(done) {
      var name = randomName()
      assertInvalidArgument({StreamName: name, NextToken: name},
        'NextToken and StreamName cannot be provided together.', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name = randomName()
      assertNotFound({StreamName: name}, 'Stream ' + name + ' under account ' +
        helpers.awsAccountId + ' not found.', done)
    })

  })

  describe('functionality', function() {

    it('should return stream shards', function(done) {
      request(opts({StreamName: helpers.testStream}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        res.body.Shards[0].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d{56}$/)
        res.body.Shards[1].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d{56}$/)
        res.body.Shards[2].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d{56}$/)

        delete res.body.Shards[0].SequenceNumberRange.StartingSequenceNumber
        delete res.body.Shards[1].SequenceNumberRange.StartingSequenceNumber
        delete res.body.Shards[2].SequenceNumberRange.StartingSequenceNumber

        res.body.should.eql({
          Shards: [{
            ShardId: 'shardId-000000000000',
            SequenceNumberRange: {},
            HashKeyRange: {
              StartingHashKey: '0',
              EndingHashKey: '113427455640312821154458202477256070484',
            },
          }, {
            ShardId: 'shardId-000000000001',
            SequenceNumberRange: {},
            HashKeyRange: {
              StartingHashKey: '113427455640312821154458202477256070485',
              EndingHashKey: '226854911280625642308916404954512140969',
            },
          }, {
            ShardId: 'shardId-000000000002',
            SequenceNumberRange: {},
            HashKeyRange: {
              StartingHashKey: '226854911280625642308916404954512140970',
              EndingHashKey: '340282366920938463463374607431768211455',
            },
          }],
        })

        done()
      })
    })

  })

})

