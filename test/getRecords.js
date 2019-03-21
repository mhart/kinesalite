var crypto = require('crypto'),
    BigNumber = require('bignumber.js'),
    helpers = require('./helpers')

var target = 'GetRecords',
    request = helpers.request,
    randomName = helpers.randomName,
    opts = helpers.opts.bind(null, target),
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('getRecords', function() {

  describe('serializations', function() {

    it('should return SerializationException when Limit is not an Integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when ShardIterator is not a String', function(done) {
      assertType('ShardIterator', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no ShardIterator', function(done) {
      assertValidation({}, [
        'Value null at \'shardIterator\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty ShardIterator', function(done) {
      assertValidation({ShardIterator: '', Limit: 0}, [
        'Value \'0\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1',
        'Value \'\' at \'shardIterator\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long ShardIterator', function(done) {
      var name = new Array(513 + 1).join('a')
      assertValidation({ShardIterator: name, Limit: 100000}, [
        'Value \'100000\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value less than or equal to 10000',
        'Value \'' + name + '\' at \'shardIterator\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 512',
      ], done)
    })

    it('should return InvalidArgumentException if ShardIterator is incorrect format', function(done) {
      var name = randomName()
      assertInvalidArgument({ShardIterator: name}, 'Invalid ShardIterator.', done)
    })

    // Takes 5 minutes to run
    it('should return ExpiredIteratorException if ShardIterator has expired', function(done) {
      this.timeout(310000)
      request(helpers.opts('GetShardIterator', {
        StreamName: helpers.testStream,
        ShardId: 'shardId-0',
        ShardIteratorType: 'LATEST',
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        setTimeout(function() {
          request(opts({ShardIterator: res.body.ShardIterator}), function(err, res) {
            if (err) return done(err)

            res.statusCode.should.equal(400)
            res.body.__type.should.equal('ExpiredIteratorException')
            res.body.message.should.match(new RegExp('^Iterator expired\\. ' +
              'The iterator was created at time \\w{3} \\w{3} \\d{2} \\d{2}:\\d{2}:\\d{2} UTC \\d{4} ' +
              'while right now it is \\w{3} \\w{3} \\d{2} \\d{2}:\\d{2}:\\d{2} UTC \\d{4} ' +
              'which is further in the future than the tolerated delay of 300000 milliseconds\\.$'))

            done()
          })
        }, 300000)
      })
    })

    // Takes 95 secs to run on production
    it('should return ResourceNotFoundException if shard or stream does not exist', function(done) {
      this.timeout(200000)
      var stream = {StreamName: randomName(), ShardCount: 2}
      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          request(helpers.opts('GetShardIterator', {
            StreamName: stream.StreamName,
            ShardId: 'shardId-0',
            ShardIteratorType: 'LATEST',
          }), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            var shardIterator0 = res.body.ShardIterator

            request(helpers.opts('GetShardIterator', {
              StreamName: stream.StreamName,
              ShardId: 'shardId-1',
              ShardIteratorType: 'LATEST',
            }), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              var shardIterator1 = res.body.ShardIterator

              request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                helpers.waitUntilDeleted(stream.StreamName, function(err, res) {
                  if (err) return done(err)
                  res.body.__type.should.equal('ResourceNotFoundException')

                  assertNotFound({ShardIterator: shardIterator0},
                      'Shard shardId-000000000000 in stream ' + stream.StreamName +
                      ' under account ' + helpers.awsAccountId + ' does not exist', function(err) {
                    if (err) return done(err)

                    assertNotFound({ShardIterator: shardIterator1},
                        'Shard shardId-000000000001 in stream ' + stream.StreamName +
                        ' under account ' + helpers.awsAccountId + ' does not exist', function(err) {
                      if (err) return done(err)

                      stream.ShardCount = 1
                      request(helpers.opts('CreateStream', stream), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)

                        helpers.waitUntilActive(stream.StreamName, function(err, res) {
                          if (err) return done(err)
                          res.statusCode.should.equal(200)

                          assertNotFound({ShardIterator: shardIterator1},
                              'Shard shardId-000000000001 in stream ' + stream.StreamName +
                              ' under account ' + helpers.awsAccountId + ' does not exist', function(err) {
                            if (err) return done(err)

                            // TODO: This now causes an InternalFailure on production
                            // request(opts({ShardIterator: shardIterator0}), function(err, res) {
                              // if (err) return done(err)
                              // console.log(res.body)
                              // res.statusCode.should.equal(200)

                              // res.body.Records.should.eql([])
                              // helpers.assertShardIterator(res.body.NextShardIterator, stream.StreamName)

                              // request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
                            // })

                            request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
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
      })
    })
  })

  describe('functionality', function() {

    it('should return correct AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER records', function(done) {
      var hashKey1 = new BigNumber(2).pow(128).minus(1).toFixed(),
        hashKey2 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).minus(1).toFixed(),
        hashKey3 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).toFixed(),
        records = [
          {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64')},
          {PartitionKey: 'b', Data: crypto.randomBytes(10).toString('base64')},
          {PartitionKey: 'e', Data: crypto.randomBytes(10).toString('base64')},
          {PartitionKey: 'f', Data: crypto.randomBytes(10).toString('base64')},
          {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey1},
          {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey2},
          {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey3},
        ]
      request(helpers.opts('PutRecords', {StreamName: helpers.testStream, Records: records}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var recordsPut = res.body.Records

        request(helpers.opts('GetShardIterator', {
          StreamName: helpers.testStream,
          ShardId: 'shardId-1',
          ShardIteratorType: 'AT_SEQUENCE_NUMBER',
          StartingSequenceNumber: recordsPut[1].SequenceNumber,
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          request(opts({ShardIterator: res.body.ShardIterator}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
            delete res.body.NextShardIterator

            res.body.MillisBehindLatest.should.be.within(0, 5000)
            delete res.body.MillisBehindLatest

            helpers.assertArrivalTimes(res.body.Records)
            res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

            res.body.should.eql({
              Records: [
                {
                  PartitionKey: records[1].PartitionKey,
                  Data: records[1].Data,
                  SequenceNumber: recordsPut[1].SequenceNumber,
                },
                {
                  PartitionKey: records[3].PartitionKey,
                  Data: records[3].Data,
                  SequenceNumber: recordsPut[3].SequenceNumber,
                },
                {
                  PartitionKey: records[5].PartitionKey,
                  Data: records[5].Data,
                  SequenceNumber: recordsPut[5].SequenceNumber,
                },
              ],
            })

            request(helpers.opts('GetShardIterator', {
              StreamName: helpers.testStream,
              ShardId: 'shardId-1',
              ShardIteratorType: 'AFTER_SEQUENCE_NUMBER',
              StartingSequenceNumber: recordsPut[1].SequenceNumber,
            }), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              request(opts({ShardIterator: res.body.ShardIterator}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                delete res.body.NextShardIterator

                res.body.MillisBehindLatest.should.be.within(0, 5000)
                delete res.body.MillisBehindLatest

                helpers.assertArrivalTimes(res.body.Records)
                res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

                res.body.should.eql({
                  Records: [
                    {
                      PartitionKey: records[3].PartitionKey,
                      Data: records[3].Data,
                      SequenceNumber: recordsPut[3].SequenceNumber,
                    },
                    {
                      PartitionKey: records[5].PartitionKey,
                      Data: records[5].Data,
                      SequenceNumber: recordsPut[5].SequenceNumber,
                    },
                  ],
                })

                done()
              })
            })
          })
        })
      })
    })

    it('should return correct AT_TIMESTAMP records', function(done) {
      var hashKey1 = new BigNumber(2).pow(128).minus(1).toFixed(),
        hashKey2 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).minus(1).toFixed(),
        hashKey3 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).toFixed(),
        records1 = [
          {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64')},
          {PartitionKey: 'b', Data: crypto.randomBytes(10).toString('base64')},
          {PartitionKey: 'e', Data: crypto.randomBytes(10).toString('base64')},
          {PartitionKey: 'f', Data: crypto.randomBytes(10).toString('base64')},
          {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey1},
          {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey2},
          {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey3},
        ]
      request(helpers.opts('PutRecords', {StreamName: helpers.testStream, Records: records1}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var secondInsertSeconds = new Date().getTime() / 1000;

        var hashKey1 = new BigNumber(2).pow(128).minus(1).toFixed(),
          hashKey2 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).minus(1).toFixed(),
          hashKey3 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).toFixed(),
          records2 = [
            {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64')},
            {PartitionKey: 'b', Data: crypto.randomBytes(10).toString('base64')},
            {PartitionKey: 'e', Data: crypto.randomBytes(10).toString('base64')},
            {PartitionKey: 'f', Data: crypto.randomBytes(10).toString('base64')},
            {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey1},
            {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey2},
            {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey3},
          ]

        request(helpers.opts('PutRecords', {StreamName: helpers.testStream, Records: records2}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          var recordsPut2 = res.body.Records

          request(helpers.opts('GetShardIterator', {
            StreamName: helpers.testStream,
            ShardId: 'shardId-1',
            ShardIteratorType: 'AT_TIMESTAMP',
            Timestamp: secondInsertSeconds
          }), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            var shardIterator = res.body.ShardIterator

            request(opts({ShardIterator: shardIterator}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              delete res.body.MillisBehindLatest

              helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
              delete res.body.NextShardIterator

              helpers.assertArrivalTimes(res.body.Records)
              res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

              res.body.should.eql({
                Records: [
                  {
                    PartitionKey: records2[1].PartitionKey,
                    Data: records2[1].Data,
                    SequenceNumber: recordsPut2[1].SequenceNumber,
                  },
                  {
                    PartitionKey: records2[3].PartitionKey,
                    Data: records2[3].Data,
                    SequenceNumber: recordsPut2[3].SequenceNumber,
                  },
                  {
                    PartitionKey: records2[5].PartitionKey,
                    Data: records2[5].Data,
                    SequenceNumber: recordsPut2[5].SequenceNumber,
                  },
                ],
              })

              done()
            })
          })
        })
      })
    })

    it('should return LATEST records', function(done) {
      request(helpers.opts('GetShardIterator', {
        StreamName: helpers.testStream,
        ShardId: 'shardId-2',
        ShardIteratorType: 'LATEST',
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var shardIterator = res.body.ShardIterator

        request(opts({ShardIterator: shardIterator}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          var nextIterator = res.body.NextShardIterator
          helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
          delete res.body.NextShardIterator

          res.body.MillisBehindLatest.should.be.within(0, 5000)
          delete res.body.MillisBehindLatest

          res.body.should.eql({Records: []})

          var hashKey1 = new BigNumber(2).pow(128).minus(1).toFixed(),
            hashKey2 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).minus(1).toFixed(),
            hashKey3 = new BigNumber(2).pow(128).div(3).integerValue(BigNumber.ROUND_FLOOR).times(2).toFixed(),
            records = [
              {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64')},
              {PartitionKey: 'b', Data: crypto.randomBytes(10).toString('base64')},
              {PartitionKey: 'e', Data: crypto.randomBytes(10).toString('base64')},
              {PartitionKey: 'f', Data: crypto.randomBytes(10).toString('base64')},
              {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey1},
              {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey2},
              {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: hashKey3},
            ]
          request(helpers.opts('PutRecords', {StreamName: helpers.testStream, Records: records}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            var recordsPut = res.body.Records

            request(opts({ShardIterator: shardIterator}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
              delete res.body.NextShardIterator

              res.body.MillisBehindLatest.should.be.within(0, 5000)
              delete res.body.MillisBehindLatest

              helpers.assertArrivalTimes(res.body.Records)
              res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

              res.body.should.eql({
                Records: [
                  {
                    PartitionKey: records[2].PartitionKey,
                    Data: records[2].Data,
                    SequenceNumber: recordsPut[2].SequenceNumber,
                  },
                  {
                    PartitionKey: records[4].PartitionKey,
                    Data: records[4].Data,
                    SequenceNumber: recordsPut[4].SequenceNumber,
                  },
                  {
                    PartitionKey: records[6].PartitionKey,
                    Data: records[6].Data,
                    SequenceNumber: recordsPut[6].SequenceNumber,
                  },
                ],
              })

              request(opts({ShardIterator: nextIterator}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                delete res.body.NextShardIterator

                res.body.MillisBehindLatest.should.be.within(0, 5000)
                delete res.body.MillisBehindLatest

                helpers.assertArrivalTimes(res.body.Records)
                res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

                res.body.should.eql({
                  Records: [
                    {
                      PartitionKey: records[2].PartitionKey,
                      Data: records[2].Data,
                      SequenceNumber: recordsPut[2].SequenceNumber,
                    },
                    {
                      PartitionKey: records[4].PartitionKey,
                      Data: records[4].Data,
                      SequenceNumber: recordsPut[4].SequenceNumber,
                    },
                    {
                      PartitionKey: records[6].PartitionKey,
                      Data: records[6].Data,
                      SequenceNumber: recordsPut[6].SequenceNumber,
                    },
                  ],
                })

                done()
              })
            })
          })
        })
      })
    })

    // Takes 35 secs to run on production
    it('should return TRIM_HORIZON records', function(done) {
      this.timeout(100000)
      var stream = {StreamName: randomName(), ShardCount: 1}
      request(helpers.opts('CreateStream', stream), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          request(helpers.opts('GetShardIterator', {
            StreamName: stream.StreamName,
            ShardId: 'shardId-0',
            ShardIteratorType: 'TRIM_HORIZON',
          }), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            var shardIterator = res.body.ShardIterator

            request(opts({ShardIterator: shardIterator}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              var nextIterator = res.body.NextShardIterator
              helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
              delete res.body.NextShardIterator

              res.body.MillisBehindLatest.should.be.within(0, 5000)
              delete res.body.MillisBehindLatest

              res.body.should.eql({Records: []})

              var record = {
                StreamName: stream.StreamName,
                PartitionKey: 'a',
                Data: crypto.randomBytes(10).toString('base64'),
                ExplicitHashKey: '0',
              }

              request(helpers.opts('PutRecord', record), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                var seqNo = res.body.SequenceNumber

                request(opts({ShardIterator: shardIterator}), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)

                  helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                  delete res.body.NextShardIterator

                  res.body.MillisBehindLatest.should.be.within(0, 5000)
                  delete res.body.MillisBehindLatest

                  helpers.assertArrivalTimes(res.body.Records)
                  res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

                  res.body.should.eql({
                    Records: [
                      {
                        PartitionKey: record.PartitionKey,
                        Data: record.Data,
                        SequenceNumber: seqNo,
                      },
                    ],
                  })

                  request(opts({ShardIterator: nextIterator}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)

                    helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                    delete res.body.NextShardIterator

                    res.body.MillisBehindLatest.should.be.within(0, 5000)
                    delete res.body.MillisBehindLatest

                    helpers.assertArrivalTimes(res.body.Records)
                    res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

                    res.body.should.eql({
                      Records: [
                        {
                          PartitionKey: record.PartitionKey,
                          Data: record.Data,
                          SequenceNumber: seqNo,
                        },
                      ],
                    })

                    request(helpers.opts('GetShardIterator', {
                      StreamName: stream.StreamName,
                      ShardId: 'shardId-0',
                      ShardIteratorType: 'TRIM_HORIZON',
                    }), function(err, res) {
                      if (err) return done(err)
                      res.statusCode.should.equal(200)

                      shardIterator = res.body.ShardIterator

                      request(opts({ShardIterator: shardIterator}), function(err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)

                        nextIterator = res.body.NextShardIterator
                        helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                        delete res.body.NextShardIterator

                        res.body.MillisBehindLatest.should.be.within(0, 5000)
                        delete res.body.MillisBehindLatest

                        helpers.assertArrivalTimes(res.body.Records)
                        res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

                        res.body.should.eql({
                          Records: [
                            {
                              PartitionKey: record.PartitionKey,
                              Data: record.Data,
                              SequenceNumber: seqNo,
                            },
                          ],
                        })

                        request(opts({ShardIterator: nextIterator}), function(err, res) {
                          if (err) return done(err)
                          res.statusCode.should.equal(200)

                          helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                          delete res.body.NextShardIterator

                          res.body.MillisBehindLatest.should.be.within(0, 5000)
                          delete res.body.MillisBehindLatest

                          res.body.should.eql({Records: []})

                          request(helpers.opts('DeleteStream', {StreamName: stream.StreamName}), done)
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
    })

    it('Should return all records with 5 MB and less than 10000 recrods limit', function (done) {
      this.timeout(50000)
      var stream = {StreamName: randomName(), ShardCount: 1}
      request(helpers.opts('CreateStream', stream), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          //
          var recordsList = [
            {PartitionKey: 'a', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'},
            {PartitionKey: 'b', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'},
            {PartitionKey: 'c', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'},
            {PartitionKey: 'd', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'},
            {PartitionKey: 'e', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'}
          ]


          request(helpers.opts('PutRecords', {
            StreamName: stream.StreamName,
            Records: recordsList
          }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            var recordsPut = res.body.Records
            request(helpers.opts('GetShardIterator', {
              StreamName: stream.StreamName,
              ShardId: 'shardId-0',
              ShardIteratorType: 'TRIM_HORIZON'
            }), function (err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              request(opts({ShardIterator: res.body.ShardIterator}), function (err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                var nextIterator = res.body.NextShardIterator
                helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                delete res.body.NextShardIterator

                res.body.MillisBehindLatest.should.be.within(0, 5000)
                delete res.body.MillisBehindLatest

                // Not checking arrival time due to the high record size.
                res.body.Records.forEach(function (record) {
                  delete record.ApproximateArrivalTimestamp
                })
                res.body.should.eql({
                  Records: [
                    {
                      PartitionKey: recordsList[0].PartitionKey,
                      Data: recordsList[0].Data,
                      SequenceNumber: recordsPut[0].SequenceNumber,
                    },
                    {
                      PartitionKey: recordsList[1].PartitionKey,
                      Data: recordsList[1].Data,
                      SequenceNumber: recordsPut[1].SequenceNumber,
                    },
                    {
                      PartitionKey: recordsList[2].PartitionKey,
                      Data: recordsList[2].Data,
                      SequenceNumber: recordsPut[2].SequenceNumber,
                    },
                    {
                      PartitionKey: recordsList[3].PartitionKey,
                      Data: recordsList[3].Data,
                      SequenceNumber: recordsPut[3].SequenceNumber,
                    },
                    {
                      PartitionKey: recordsList[4].PartitionKey,
                      Data: recordsList[4].Data,
                      SequenceNumber: recordsPut[4].SequenceNumber,
                    },
                  ],
                })
                request(opts({ShardIterator: nextIterator}), function (err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)

                  var nextIterator = res.body.NextShardIterator
                  helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                  delete res.body.NextShardIterator
                  should.equal(0, res.body.Records.length)
                  done()
                })
              })
            })
          })
        })
      })
    })

    it('Should return max of 5 MB for each get records', function (done) {
      this.timeout(50000)
      var stream = {StreamName: randomName(), ShardCount: 1}
      request(helpers.opts('CreateStream', stream), function (err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        helpers.waitUntilActive(stream.StreamName, function (err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          //
          var recordsList1 = [
            {PartitionKey: 'a', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'},
            {PartitionKey: 'b', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'},
            {PartitionKey: 'c', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'},
            {PartitionKey: 'd', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'},
            {PartitionKey: 'e', Data: crypto.randomBytes(1000000).toString('base64'), ExplicitHashKey: '0'}
          ]


          request(helpers.opts('PutRecords', {
            StreamName: stream.StreamName,
            Records: recordsList1
          }), function (err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            var recordsPut1 = res.body.Records
            var recordsList2 = [
              {
                PartitionKey: 'f',
                Data: crypto.randomBytes(1000000).toString('base64'),
                ExplicitHashKey: '0'
              },
              {
                PartitionKey: 'g',
                Data: crypto.randomBytes(1000000).toString('base64'),
                ExplicitHashKey: '0'
              },
              {
                PartitionKey: 'h',
                Data: crypto.randomBytes(1000000).toString('base64'),
                ExplicitHashKey: '0'
              },
              {
                PartitionKey: 'i',
                Data: crypto.randomBytes(1000000).toString('base64'),
                ExplicitHashKey: '0'
              },
              {
                PartitionKey: 'j',
                Data: crypto.randomBytes(1000000).toString('base64'),
                ExplicitHashKey: '0'
              },
            ]
            request(helpers.opts('PutRecords', {
              StreamName: stream.StreamName,
              Records: recordsList2
            }), function (err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              var recordsPut2 = res.body.Records

              var recordsList3 = [
                {
                  PartitionKey: 'k',
                  Data: crypto.randomBytes(1000000).toString('base64'),
                  ExplicitHashKey: '0'
                },
                {
                  PartitionKey: 'l',
                  Data: crypto.randomBytes(1000000).toString('base64'),
                  ExplicitHashKey: '0'
                },
                {
                  PartitionKey: 'm',
                  Data: crypto.randomBytes(1000000).toString('base64'),
                  ExplicitHashKey: '0'
                },
                {
                  PartitionKey: 'n',
                  Data: crypto.randomBytes(1000000).toString('base64'),
                  ExplicitHashKey: '0'
                },
                {
                  PartitionKey: 'o',
                  Data: crypto.randomBytes(1000000).toString('base64'),
                  ExplicitHashKey: '0'
                },
              ]

              request(helpers.opts('PutRecords', {
                StreamName: stream.StreamName,
                Records: recordsList3
              }), function (err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                var recordsPut3 = res.body.Records
                request(helpers.opts('GetShardIterator', {
                  StreamName: stream.StreamName,
                  ShardId: 'shardId-0',
                  ShardIteratorType: 'TRIM_HORIZON'
                }), function (err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)

                  request(opts({ShardIterator: res.body.ShardIterator}), function (err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)

                    var nextIterator = res.body.NextShardIterator
                    helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                    delete res.body.NextShardIterator

                    res.body.MillisBehindLatest.should.be.within(0, 15000)
                    delete res.body.MillisBehindLatest

                    // Not checking arrival time due to the high record size.
                    res.body.Records.forEach(function (record) {
                      delete record.ApproximateArrivalTimestamp
                    })
                    res.body.should.eql({
                      Records: [
                        {
                          PartitionKey: recordsList1[0].PartitionKey,
                          Data: recordsList1[0].Data,
                          SequenceNumber: recordsPut1[0].SequenceNumber,
                        },
                        {
                          PartitionKey: recordsList1[1].PartitionKey,
                          Data: recordsList1[1].Data,
                          SequenceNumber: recordsPut1[1].SequenceNumber,
                        },
                        {
                          PartitionKey: recordsList1[2].PartitionKey,
                          Data: recordsList1[2].Data,
                          SequenceNumber: recordsPut1[2].SequenceNumber,
                        },
                        {
                          PartitionKey: recordsList1[3].PartitionKey,
                          Data: recordsList1[3].Data,
                          SequenceNumber: recordsPut1[3].SequenceNumber,
                        },
                        {
                          PartitionKey: recordsList1[4].PartitionKey,
                          Data: recordsList1[4].Data,
                          SequenceNumber: recordsPut1[4].SequenceNumber,
                        },
                      ],
                    })
                    request(opts({ShardIterator: nextIterator}), function (err, res) {
                      if (err) return done(err)
                      res.statusCode.should.equal(200)

                      helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                      var nextIterator = res.body.NextShardIterator
                      delete res.body.NextShardIterator

                      res.body.MillisBehindLatest.should.be.within(0, 15000)
                      delete res.body.MillisBehindLatest

                      res.body.Records.forEach(function (record) {
                        delete record.ApproximateArrivalTimestamp
                      })

                      res.body.should.eql({
                        Records: [
                          {
                            PartitionKey: recordsList2[0].PartitionKey,
                            Data: recordsList2[0].Data,
                            SequenceNumber: recordsPut2[0].SequenceNumber,
                          },
                          {
                            PartitionKey: recordsList2[1].PartitionKey,
                            Data: recordsList2[1].Data,
                            SequenceNumber: recordsPut2[1].SequenceNumber,
                          },
                          {
                            PartitionKey: recordsList2[2].PartitionKey,
                            Data: recordsList2[2].Data,
                            SequenceNumber: recordsPut2[2].SequenceNumber,
                          },
                          {
                            PartitionKey: recordsList2[3].PartitionKey,
                            Data: recordsList2[3].Data,
                            SequenceNumber: recordsPut2[3].SequenceNumber,
                          },
                          {
                            PartitionKey: recordsList2[4].PartitionKey,
                            Data: recordsList2[4].Data,
                            SequenceNumber: recordsPut2[4].SequenceNumber,
                          },
                        ],
                      })
                      request(opts({ShardIterator: nextIterator}), function (err, res) {
                        if (err) return done(err)
                        res.statusCode.should.equal(200)

                        helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                        delete res.body.NextShardIterator

                        res.body.MillisBehindLatest.should.be.within(0, 15000)
                        delete res.body.MillisBehindLatest

                        res.body.Records.forEach(function (record) {
                          delete record.ApproximateArrivalTimestamp
                        })

                        res.body.should.eql({
                          Records: [
                            {
                              PartitionKey: recordsList3[0].PartitionKey,
                              Data: recordsList3[0].Data,
                              SequenceNumber: recordsPut3[0].SequenceNumber,
                            },
                            {
                              PartitionKey: recordsList3[1].PartitionKey,
                              Data: recordsList3[1].Data,
                              SequenceNumber: recordsPut3[1].SequenceNumber,
                            },
                            {
                              PartitionKey: recordsList3[2].PartitionKey,
                              Data: recordsList3[2].Data,
                              SequenceNumber: recordsPut3[2].SequenceNumber,
                            },
                            {
                              PartitionKey: recordsList3[3].PartitionKey,
                              Data: recordsList3[3].Data,
                              SequenceNumber: recordsPut3[3].SequenceNumber,
                            },
                            {
                              PartitionKey: recordsList3[4].PartitionKey,
                              Data: recordsList3[4].Data,
                              SequenceNumber: recordsPut3[4].SequenceNumber,
                            },
                          ],
                        })
                        done()
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

    it('should return correct records with Limit', function(done) {
      var records = [
        {PartitionKey: 'a', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: '0'},
        {PartitionKey: 'b', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: '0'},
        {PartitionKey: 'c', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: '0'},
        {PartitionKey: 'd', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: '0'},
        {PartitionKey: 'e', Data: crypto.randomBytes(10).toString('base64'), ExplicitHashKey: '0'},
      ]
      request(helpers.opts('PutRecords', {StreamName: helpers.testStream, Records: records}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        var recordsPut = res.body.Records

        request(helpers.opts('GetShardIterator', {
          StreamName: helpers.testStream,
          ShardId: 'shardId-0',
          ShardIteratorType: 'AT_SEQUENCE_NUMBER',
          StartingSequenceNumber: recordsPut[1].SequenceNumber,
        }), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)

          request(opts({ShardIterator: res.body.ShardIterator, Limit: 2}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)

            var nextIterator = res.body.NextShardIterator
            helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
            delete res.body.NextShardIterator

            res.body.MillisBehindLatest.should.be.within(0, 5000)
            delete res.body.MillisBehindLatest

            helpers.assertArrivalTimes(res.body.Records)
            res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

            res.body.should.eql({
              Records: [
                {
                  PartitionKey: records[1].PartitionKey,
                  Data: records[1].Data,
                  SequenceNumber: recordsPut[1].SequenceNumber,
                },
                {
                  PartitionKey: records[2].PartitionKey,
                  Data: records[2].Data,
                  SequenceNumber: recordsPut[2].SequenceNumber,
                },
              ],
            })

            request(opts({ShardIterator: nextIterator, Limit: 3}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
              delete res.body.NextShardIterator

              res.body.MillisBehindLatest.should.be.within(0, 5000)
              delete res.body.MillisBehindLatest

              helpers.assertArrivalTimes(res.body.Records)
              res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

              res.body.should.eql({
                Records: [
                  {
                    PartitionKey: records[3].PartitionKey,
                    Data: records[3].Data,
                    SequenceNumber: recordsPut[3].SequenceNumber,
                  },
                  {
                    PartitionKey: records[4].PartitionKey,
                    Data: records[4].Data,
                    SequenceNumber: recordsPut[4].SequenceNumber,
                  },
                ],
              })

              request(opts({ShardIterator: nextIterator, Limit: 1}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                delete res.body.NextShardIterator

                res.body.MillisBehindLatest.should.be.within(0, 5000)
                delete res.body.MillisBehindLatest

                helpers.assertArrivalTimes(res.body.Records)
                res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

                res.body.should.eql({
                  Records: [
                    {
                      PartitionKey: records[3].PartitionKey,
                      Data: records[3].Data,
                      SequenceNumber: recordsPut[3].SequenceNumber,
                    },
                  ],
                })

                request(opts({ShardIterator: nextIterator}), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)

                  helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)
                  delete res.body.NextShardIterator

                  res.body.MillisBehindLatest.should.be.within(0, 5000)
                  delete res.body.MillisBehindLatest

                  helpers.assertArrivalTimes(res.body.Records)
                  res.body.Records.forEach(function(record) { delete record.ApproximateArrivalTimestamp })

                  res.body.should.eql({
                    Records: [
                      {
                        PartitionKey: records[3].PartitionKey,
                        Data: records[3].Data,
                        SequenceNumber: recordsPut[3].SequenceNumber,
                      },
                      {
                        PartitionKey: records[4].PartitionKey,
                        Data: records[4].Data,
                        SequenceNumber: recordsPut[4].SequenceNumber,
                      },
                    ],
                  })
                  done()
                })
              })
            })
          })
        })
      })
    })
  })
})

