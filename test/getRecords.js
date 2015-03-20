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
      assertValidation({},
        '1 validation error detected: ' +
        'Value null at \'shardIterator\' failed to satisfy constraint: ' +
        'Member must not be null', done)
    })

    it('should return ValidationException for empty ShardIterator', function(done) {
      assertValidation({ShardIterator: '', Limit: 0},
        '2 validation errors detected: ' +
        'Value \'0\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'\' at \'shardIterator\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long ShardIterator', function(done) {
      var name = new Array(513 + 1).join('a')
      assertValidation({ShardIterator: name, Limit: 100000},
        '2 validation errors detected: ' +
        'Value \'100000\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value less than or equal to 10000; ' +
        'Value \'' + name + '\' at \'shardIterator\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 512', done)
    })

    it('should return InvalidArgumentException if ShardIterator is incorrect format', function(done) {
      var name = randomName()
      assertInvalidArgument({ShardIterator: name}, 'Invalid ShardIterator.', done)
    })

    // Takes 5 minutes to run
    it.skip('should return ExpiredIteratorException if ShardIterator has expired', function(done) {
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

                            request(opts({ShardIterator: shardIterator0}), function(err, res) {
                              if (err) return done(err)
                              res.statusCode.should.equal(200)

                              res.body.Records.should.eql([])
                              helpers.assertShardIterator(res.body.NextShardIterator, stream.StreamName)

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
  })

  describe('functionality', function() {

    it('should return correct AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER records', function(done) {
      var hashKey1 = new BigNumber(2).pow(128).minus(1).toFixed(),
        hashKey2 = new BigNumber(2).pow(128).div(3).floor().times(2).minus(1).toFixed(),
        hashKey3 = new BigNumber(2).pow(128).div(3).floor().times(2).toFixed(),
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

            res.body.Records.should.eql([
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
            ])
            helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)

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

                res.body.Records.should.eql([
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
                ])
                helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)

                done()
              })
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

          res.body.Records.should.eql([])
          helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)

          var nextIterator = res.body.NextShardIterator

          var hashKey1 = new BigNumber(2).pow(128).minus(1).toFixed(),
            hashKey2 = new BigNumber(2).pow(128).div(3).floor().times(2).minus(1).toFixed(),
            hashKey3 = new BigNumber(2).pow(128).div(3).floor().times(2).toFixed(),
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

              res.body.Records.should.eql([
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
              ])
              helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)

              request(opts({ShardIterator: nextIterator}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                res.body.Records.should.eql([
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
                ])
                helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)

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

              res.body.Records.should.eql([])
              helpers.assertShardIterator(res.body.NextShardIterator, stream.StreamName)

              var nextIterator = res.body.NextShardIterator

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

                  res.body.Records.should.eql([
                    {
                      PartitionKey: record.PartitionKey,
                      Data: record.Data,
                      SequenceNumber: seqNo,
                    },
                  ])
                  helpers.assertShardIterator(res.body.NextShardIterator, stream.StreamName)

                  request(opts({ShardIterator: nextIterator}), function(err, res) {
                    if (err) return done(err)
                    res.statusCode.should.equal(200)

                    res.body.Records.should.eql([
                      {
                        PartitionKey: record.PartitionKey,
                        Data: record.Data,
                        SequenceNumber: seqNo,
                      },
                    ])
                    helpers.assertShardIterator(res.body.NextShardIterator, stream.StreamName)

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

                        res.body.Records.should.eql([
                          {
                            PartitionKey: record.PartitionKey,
                            Data: record.Data,
                            SequenceNumber: seqNo,
                          },
                        ])
                        helpers.assertShardIterator(res.body.NextShardIterator, stream.StreamName)

                        var nextIterator = res.body.NextShardIterator

                        request(opts({ShardIterator: nextIterator}), function(err, res) {
                          if (err) return done(err)
                          res.statusCode.should.equal(200)

                          res.body.Records.should.eql([])
                          helpers.assertShardIterator(res.body.NextShardIterator, stream.StreamName)

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

            res.body.Records.should.eql([
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
            ])
            helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)

            var nextIterator = res.body.NextShardIterator

            request(opts({ShardIterator: nextIterator, Limit: 3}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              res.body.Records.should.eql([
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
              ])
              helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)

              request(opts({ShardIterator: nextIterator, Limit: 1}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)

                res.body.Records.should.eql([
                  {
                    PartitionKey: records[3].PartitionKey,
                    Data: records[3].Data,
                    SequenceNumber: recordsPut[3].SequenceNumber,
                  },
                ])
                helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)

                request(opts({ShardIterator: nextIterator}), function(err, res) {
                  if (err) return done(err)
                  res.statusCode.should.equal(200)

                  res.body.Records.should.eql([
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
                  ])
                  helpers.assertShardIterator(res.body.NextShardIterator, helpers.testStream)

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

