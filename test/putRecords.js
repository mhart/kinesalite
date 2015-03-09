var BigNumber = require('bignumber.js'),
    helpers = require('./helpers')

var target = 'PutRecords',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('putRecords', function() {

  describe('serializations', function() {

    it('should return SerializationException when Records is not a list', function(done) {
      assertType('Records', 'List', done)
    })

    it('should return SerializationException when Records.0 is not a struct', function(done) {
      assertType('Records.0', 'Structure', done)
    })

    it('should return SerializationException when Records.0.Data is not a Blob', function(done) {
      assertType('Records.0.Data', 'Blob', done)
    })

    it('should return SerializationException when Records.0.ExplicitHashKey is not a String', function(done) {
      assertType('Records.0.ExplicitHashKey', 'String', done)
    })

    it('should return SerializationException when Records.0.PartitionKey is not a String', function(done) {
      assertType('Records.0.PartitionKey', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({},
        '2 validation errors detected: ' +
        'Value null at \'records\' failed to satisfy constraint: ' +
        'Member must not be null; ' +
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', Records: []},
        '3 validation errors detected: ' +
        'Value \'[]\' at \'records\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for empty PartitionKey', function(done) {
      assertValidation({StreamName: '', Records: [{PartitionKey: '', Data: '', ExplicitHashKey: ''}]},
        '4 validation errors detected: ' +
        'Value \'\' at \'records.1.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'records.1.member.explicitHashKey\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,38}); ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a'), name2 = new Array(257 + 1).join('a'),
        data = new Buffer(51201).toString('base64')
      assertValidation({StreamName: name, Records: [{PartitionKey: name2, Data: data, ExplicitHashKey: ''}]},
        '4 validation errors detected: ' +
        'Value \'' + name2 + '\' at \'records.1.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 256; ' +
        'Value \'\' at \'records.1.member.explicitHashKey\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,38}); ' +
        'Value \'java.nio.HeapByteBuffer[pos=0 lim=51201 cap=51201]\' at \'records.1.member.data\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 51200; ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return ValidationException for too many records', function(done) {
      var records = [], strs = []
      for (var i = 0; i < 501; i++) {
        records.push({PartitionKey: '', Data: ''})
        strs.push('com.amazonaws.kinesis.v20131202.PutRecordsRequestEntry@2')
      }
      assertValidation({StreamName: '', Records: records},
        '10 validation errors detected: ' +
        'Value \'[' + strs.join(', ') + ']\' at \'records\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 500; ' +
        'Value \'\' at \'records.1.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'records.2.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'records.3.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'records.4.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'records.5.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'records.6.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'records.7.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'records.8.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'records.9.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = randomName()
      assertNotFound({StreamName: name1, Records: [{PartitionKey: 'a', Data: ''}]},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

    it('should return InvalidArgumentException for out of bounds ExplicitHashKey', function(done) {
      var hashKey = new BigNumber(2).pow(128).toFixed()
      assertInvalidArgument({StreamName: helpers.testStream, Records: [
          {PartitionKey: 'a', Data: '', ExplicitHashKey: hashKey}, {PartitionKey: 'a', Data: ''}]},
        'Invalid ExplicitHashKey. ExplicitHashKey must be in the range: [0, 2^128-1]. Specified value was ' + hashKey, done)
    })

  })

  describe('functionality', function() {

    it('should work with mixed values', function(done) {
      var now = Date.now(),
        hashKey1 = new BigNumber(2).pow(128).minus(1).toFixed(),
        hashKey2 = new BigNumber(2).pow(128).div(3).floor().times(2).minus(1).toFixed(),
        hashKey3 = new BigNumber(2).pow(128).div(3).floor().times(2).toFixed(),
        records = [
          {PartitionKey: 'a', Data: ''},
          {PartitionKey: 'b', Data: ''},
          {PartitionKey: 'e', Data: ''},
          {PartitionKey: 'f', Data: ''},
          {PartitionKey: 'a', Data: '', ExplicitHashKey: hashKey1},
          {PartitionKey: 'a', Data: '', ExplicitHashKey: hashKey2},
          {PartitionKey: 'a', Data: '', ExplicitHashKey: hashKey3},
        ]
      request(opts({StreamName: helpers.testStream, Records: records}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.FailedRecordCount.should.equal(0)
        res.body.Records.should.have.length(records.length)

        res.body.Records[0].ShardId.should.equal('shardId-000000000000')
        helpers.assertSequenceNumber(res.body.Records[0].SequenceNumber, 0, now)
        res.body.Records[1].ShardId.should.equal('shardId-000000000001')
        helpers.assertSequenceNumber(res.body.Records[1].SequenceNumber, 1, now)
        res.body.Records[2].ShardId.should.equal('shardId-000000000002')
        helpers.assertSequenceNumber(res.body.Records[2].SequenceNumber, 2, now)
        res.body.Records[3].ShardId.should.equal('shardId-000000000001')
        helpers.assertSequenceNumber(res.body.Records[3].SequenceNumber, 1, now)
        res.body.Records[4].ShardId.should.equal('shardId-000000000002')
        helpers.assertSequenceNumber(res.body.Records[4].SequenceNumber, 2, now)
        res.body.Records[5].ShardId.should.equal('shardId-000000000001')
        helpers.assertSequenceNumber(res.body.Records[5].SequenceNumber, 1, now)
        res.body.Records[6].ShardId.should.equal('shardId-000000000002')
        helpers.assertSequenceNumber(res.body.Records[6].SequenceNumber, 2, now)

        var indexOrder = [1, 3, 5, 0, 2, 4, 6], lastIx
        indexOrder.forEach(function(i) {
          var seqIx = parseInt(new BigNumber(res.body.Records[i].SequenceNumber).toString(16).slice(11, 27), 16),
            diff = lastIx != null ? seqIx - lastIx : 1
          diff.should.equal(1)
          lastIx = seqIx
        })

        done()
      })
    })
  })
})
