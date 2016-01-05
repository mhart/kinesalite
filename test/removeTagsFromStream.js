var helpers = require('./helpers')

var target = 'RemoveTagsFromStream',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    randomName = helpers.randomName,
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('removeTagsFromStream', function() {

  describe('serializations', function() {

    it('should return SerializationException when TagKeys is not a list', function(done) {
      assertType('TagKeys', 'List', done)
    })

    it('should return SerializationException when TagKeys.0 is not a string', function(done) {
      assertType('TagKeys.0', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({}, [
        'Value null at \'tagKeys\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', TagKeys: []}, [
        'Value \'[]\' at \'tagKeys\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, TagKeys: ['a']}, [
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return ValidationException for long TagKey', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: randomName(), TagKeys: ['a', name, 'b']}, [
        'Value \'[a, ' + name + ', b]\' at \'tagKeys\' failed to satisfy constraint: ' +
        'Member must satisfy constraint: [Member must have length less than or equal to 128, ' +
        'Member must have length greater than or equal to 1]',
      ], done)
    })

    it('should return ValidationException for too many TagKeys', function(done) {
      assertValidation({StreamName: randomName(), TagKeys: ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11']}, [
        'Value \'[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]\' at \'tagKeys\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 10',
      ], done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = randomName()
      assertNotFound({StreamName: name1, TagKeys: [';']},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

    it('should return InvalidArgumentException if ; in TagKeys', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, TagKeys: ['abc;def']},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if tab in TagKeys', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, TagKeys: ['abc\tdef']},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if newline in TagKeys', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, TagKeys: ['abc\ndef']},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if comma in TagKeys', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, TagKeys: ['abc,def']},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if % in TagKeys', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, TagKeys: ['abc%def']},
        'Failed to remove tags from stream ' + helpers.testStream + ' under account ' + helpers.awsAccountId +
        ' because some tags contained illegal characters. The allowed characters are ' +
        'Unicode letters, white-spaces, \'_\',\',\',\'/\',\'=\',\'+\',\'-\',\'@\'.', done)
    })

  })

  describe('functionality', function() {

    it('should succeed if valid characters in tag keys', function(done) {
      request(opts({StreamName: helpers.testStream, TagKeys: ['ü0 _.', '/=+-@']}), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.body.should.equal('')
        done()
      })
    })

    it('should add and remove tags keys', function(done) {
      request(helpers.opts('AddTagsToStream', {
        StreamName: helpers.testStream,
        Tags: {a: 'a', 'ü0 _.': 'a', '/=+-@': 'a'},
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        request(helpers.opts('ListTagsForStream', {StreamName: helpers.testStream}), function(err, res) {
          if (err) return done(err)
          res.statusCode.should.equal(200)
          res.body.Tags.should.containEql({Key: 'a', Value: 'a'})
          res.body.Tags.should.containEql({Key: 'ü0 _.', Value: 'a'})
          res.body.Tags.should.containEql({Key: '/=+-@', Value: 'a'})

          request(opts({StreamName: helpers.testStream, TagKeys: ['ü0 _.', '/=+-@', 'b', 'c']}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.should.equal('')

            request(helpers.opts('ListTagsForStream', {StreamName: helpers.testStream}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)
              res.body.Tags.should.containEql({Key: 'a', Value: 'a'})
              res.body.Tags.should.not.containEql({Key: 'ü0 _.', Value: 'a'})
              res.body.Tags.should.not.containEql({Key: '/=+-@', Value: 'a'})

              request(opts({StreamName: helpers.testStream, TagKeys: ['a']}), done)
            })
          })
        })
      })
    })

  })

})

