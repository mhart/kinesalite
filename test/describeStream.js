var helpers = require('./helpers')

var target = 'DescribeStream',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('describeStream', function() {

  describe('serializations', function() {

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

    it('should return SerializationException when Limit is not an Integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when ExclusiveStartShardId is not a String', function(done) {
      assertType('ExclusiveStartShardId', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({},
        '1 validation error detected: ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', Limit: 0, ExclusiveStartShardId: ''},
        '5 validation errors detected: ' +
        'Value \'0\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'\' at \'exclusiveStartShardId\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'exclusiveStartShardId\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, Limit: 100000, ExclusiveStartShardId: name},
        '3 validation errors detected: ' +
        'Value \'100000\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value less than or equal to 10000; ' +
        'Value \'' + name + '\' at \'exclusiveStartShardId\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128; ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name = randomName()
      assertNotFound({StreamName: name}, 'Stream ' + name + ' under account ' +
        helpers.awsAccountId + ' not found.', done)
    })

  })

  describe('functionality', function() {

    it('should return stream description', function(done) {
      request(opts({StreamName: helpers.testStream}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d{56}$/)
        res.body.StreamDescription.Shards[1].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d{56}$/)
        res.body.StreamDescription.Shards[2].SequenceNumberRange.StartingSequenceNumber.should.match(/^\d{56}$/)

        delete res.body.StreamDescription.Shards[0].SequenceNumberRange.StartingSequenceNumber
        delete res.body.StreamDescription.Shards[1].SequenceNumberRange.StartingSequenceNumber
        delete res.body.StreamDescription.Shards[2].SequenceNumberRange.StartingSequenceNumber

        res.body.should.eql({
          StreamDescription: {
            StreamStatus: 'ACTIVE',
            StreamName: helpers.testStream,
            StreamARN: 'arn:aws:kinesis:' + helpers.awsRegion + ':' + helpers.awsAccountId +
              ':stream/' + helpers.testStream,
            HasMoreShards: false,
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
          },
        })

        done()
      })
    })

  })

})

