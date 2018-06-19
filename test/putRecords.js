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
      assertValidation({}, [
        'Value null at \'records\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', Records: []}, [
        'Value \'[]\' at \'records\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for empty PartitionKey', function(done) {
      assertValidation({StreamName: '', Records: [{PartitionKey: '', Data: '', ExplicitHashKey: ''}]}, [
        'Value \'\' at \'records.1.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'records.1.member.explicitHashKey\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,38})',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a'), name2 = new Array(257 + 1).join('a'),
        data = new Buffer(1048577).toString('base64')
      assertValidation({StreamName: name, Records: [{PartitionKey: name2, Data: data, ExplicitHashKey: ''}]}, [
        'Value \'' + name2 + '\' at \'records.1.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 256',
        'Value \'\' at \'records.1.member.explicitHashKey\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: 0|([1-9]\\d{0,38})',
        'Value \'java.nio.HeapByteBuffer[pos=0 lim=1048577 cap=1048577]\' at \'records.1.member.data\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 1048576',
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return ValidationException for too many records', function(done) {
      var records = [], strs = []
      for (var i = 0; i < 501; i++) {
        records.push({PartitionKey: '', Data: ''})
        strs.push('com.amazonaws.kinesis.v20131202.PutRecordsRequestEntry@c965e310')
      }
      assertValidation({StreamName: '', Records: records}, [
        'Value \'[' + strs.join(', ') + ']\' at \'records\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 500',
        'Value \'\' at \'records.1.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'records.2.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'records.3.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'records.4.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'records.5.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'records.6.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'records.7.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'records.8.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'records.9.member.partitionKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
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

    it('should work with large Data', function(done) {
      var now = Date.now(), data = new Buffer(51200).toString('base64'), records = [{PartitionKey: 'a', Data: data}]
      request(opts({StreamName: helpers.testStream, Records: records}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.assertSequenceNumber(res.body.Records[0].SequenceNumber, 0, now)
        delete res.body.Records[0].SequenceNumber

        res.body.should.eql({
          FailedRecordCount: 0,
          Records: [{
            ShardId: 'shardId-000000000000',
          }],
        })

        done()
      })
    })

    it('should work with mixed values', function(done) {
      var now = Date.now(),
        hashKey1 = new BigNumber(2).pow(128).minus(1).toFixed(),
        hashKey2 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).minus(1).toFixed(),
        hashKey3 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).toFixed(),
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

        helpers.assertSequenceNumber(res.body.Records[0].SequenceNumber, 0, now)
        helpers.assertSequenceNumber(res.body.Records[1].SequenceNumber, 1, now)
        helpers.assertSequenceNumber(res.body.Records[2].SequenceNumber, 2, now)
        helpers.assertSequenceNumber(res.body.Records[3].SequenceNumber, 1, now)
        helpers.assertSequenceNumber(res.body.Records[4].SequenceNumber, 2, now)
        helpers.assertSequenceNumber(res.body.Records[5].SequenceNumber, 1, now)
        helpers.assertSequenceNumber(res.body.Records[6].SequenceNumber, 2, now)

        var indexOrder = [[2, 4, 6], [1, 3, 5], [0]]
        indexOrder.forEach(function(arr) {
          var lastIx
          arr.forEach(function(i) {
            var seqIx = parseInt(new BigNumber(res.body.Records[i].SequenceNumber).toString(16).slice(11, 27), 16),
              diff = lastIx != null ? seqIx - lastIx : 1
            diff.should.equal(1)
            lastIx = seqIx
          })
        })

        delete res.body.Records[0].SequenceNumber
        delete res.body.Records[1].SequenceNumber
        delete res.body.Records[2].SequenceNumber
        delete res.body.Records[3].SequenceNumber
        delete res.body.Records[4].SequenceNumber
        delete res.body.Records[5].SequenceNumber
        delete res.body.Records[6].SequenceNumber

        res.body.should.eql({
          FailedRecordCount: 0,
          Records: [{
            ShardId: 'shardId-000000000000',
          }, {
            ShardId: 'shardId-000000000001',
          }, {
            ShardId: 'shardId-000000000002',
          }, {
            ShardId: 'shardId-000000000001',
          }, {
            ShardId: 'shardId-000000000002',
          }, {
            ShardId: 'shardId-000000000001',
          }, {
            ShardId: 'shardId-000000000002',
          }],
        })

        done()
      })
    })

    // Use this test to play around with sequence number generation
    // aws kinesis create-stream --stream-name test --shard-count 50
    it.skip('should print out sequences for many shards', function(done) {
      var records = [], numShards = 50, streamName = 'test'
      for (var j = 0; j < 2; j++) {
        for (var i = 0; i < numShards; i++) {
          records.push({
            PartitionKey: 'a',
            Data: '',
            ExplicitHashKey: new BigNumber(2).pow(128).div(numShards).integerValue(BigNumber.ROUND_FLOOR).times(i + 1).minus(1).toFixed(),
          })
        }
      }
      request(opts({StreamName: streamName, Records: records}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        res.body.Records.sort(function(a, b) {
          var seqIxA = parseInt(new BigNumber(a.SequenceNumber).toString(16).slice(11, 27), 16)
          var seqIxB = parseInt(new BigNumber(b.SequenceNumber).toString(16).slice(11, 27), 16)
          return seqIxA - seqIxB
        })
        res.body.Records.forEach(function(record, i) {
          var seqIx = parseInt(new BigNumber(record.SequenceNumber).toString(16).slice(11, 27), 16)
          console.log(i, seqIx, record.ShardId)
        })

        done()
      })
    })
  })
})
