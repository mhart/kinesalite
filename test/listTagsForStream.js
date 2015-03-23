var helpers = require('./helpers')

var target = 'ListTagsForStream',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    randomName = helpers.randomName,
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target)

describe('listTagsForStream', function() {

  describe('serializations', function() {

    it('should return SerializationException when Limit is not an Integer', function(done) {
      assertType('Limit', 'Integer', done)
    })

    it('should return SerializationException when ExclusiveStartTagKey is not a String', function(done) {
      assertType('ExclusiveStartTagKey', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
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
      assertValidation({StreamName: '', ExclusiveStartTagKey: '', Limit: 0},
        '4 validation errors detected: ' +
        'Value \'0\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value greater than or equal to 1; ' +
        'Value \'\' at \'exclusiveStartTagKey\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+; ' +
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1', done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, ExclusiveStartTagKey: name, Limit: 100},
        '3 validation errors detected: ' +
        'Value \'100\' at \'limit\' failed to satisfy constraint: ' +
        'Member must have value less than or equal to 10; ' +
        'Value \'' + name + '\' at \'exclusiveStartTagKey\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128; ' +
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128', done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = randomName()
      assertNotFound({StreamName: name1, ExclusiveStartTagKey: 'a', Limit: 1},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

  })

  describe('functionality', function() {

    it('should return empty list by default', function(done) {
      request(opts({StreamName: helpers.testStream}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({
          Tags: [],
          HasMoreTags: false,
        })
        done()
      })
    })

    it('should return empty list with limit and start key', function(done) {
      request(opts({StreamName: helpers.testStream, ExclusiveStartTagKey: 'a', Limit: 1}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.eql({
          Tags: [],
          HasMoreTags: false,
        })
        done()
      })
    })

    it('should list in alphabetical order', function(done) {
      request(helpers.opts('AddTagsToStream', {
        StreamName: helpers.testStream,
        Tags: {a: 'b', ' ': 'a', 'ÿ': 'a', '_': 'a', '/': 'a', '=': 'a', '+': 'a', Zb: 'z', 0: 'a', '@': 'a'}, // can't do '%' or '\t'
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(opts({StreamName: helpers.testStream}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.should.eql({
            Tags: [
              {Key: ' ', Value: 'a'},
              {Key: '+', Value: 'a'},
              {Key: '/', Value: 'a'},
              {Key: '0', Value: 'a'},
              {Key: '=', Value: 'a'},
              {Key: '@', Value: 'a'},
              {Key: 'Zb', Value: 'z'},
              {Key: '_', Value: 'a'},
              {Key: 'a', Value: 'b'},
              {Key: 'ÿ', Value: 'a'},
            ],
            HasMoreTags: false,
          })

          request(opts({
            StreamName: helpers.testStream,
            ExclusiveStartTagKey: '@',
            Limit: 2,
          }), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.eql({
              Tags: [
                {Key: 'Zb', Value: 'z'},
                {Key: '_', Value: 'a'},
              ],
              HasMoreTags: true,
            })

            request(opts({
              StreamName: helpers.testStream,
              ExclusiveStartTagKey: '$Z%*(*&@,,.,,ZAC',
            }), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.should.eql({
                Tags: [
                  {Key: '+', Value: 'a'},
                  {Key: '/', Value: 'a'},
                  {Key: '0', Value: 'a'},
                  {Key: '=', Value: 'a'},
                  {Key: '@', Value: 'a'},
                  {Key: 'Zb', Value: 'z'},
                  {Key: '_', Value: 'a'},
                  {Key: 'a', Value: 'b'},
                  {Key: 'ÿ', Value: 'a'},
                ],
                HasMoreTags: false,
              })

              request(opts({
                StreamName: helpers.testStream,
                ExclusiveStartTagKey: 'Za$Z%*(*&@,,.,,',
              }), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.should.eql({
                  Tags: [
                    {Key: 'Zb', Value: 'z'},
                    {Key: '_', Value: 'a'},
                    {Key: 'a', Value: 'b'},
                    {Key: 'ÿ', Value: 'a'},
                  ],
                  HasMoreTags: false,
                })

                request(helpers.opts('RemoveTagsFromStream', {
                  StreamName: helpers.testStream,
                  TagKeys: ['a', ' ', 'ÿ', '_', '/', '=', '+', 'Zb', '0', '@'],
                }), done)
              })
            })
          })
        })
      })
    })

  })

})


