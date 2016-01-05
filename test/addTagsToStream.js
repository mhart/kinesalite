var helpers = require('./helpers')

var target = 'AddTagsToStream',
    request = helpers.request,
    opts = helpers.opts.bind(null, target),
    randomName = helpers.randomName,
    assertType = helpers.assertType.bind(null, target),
    assertValidation = helpers.assertValidation.bind(null, target),
    assertNotFound = helpers.assertNotFound.bind(null, target),
    assertInvalidArgument = helpers.assertInvalidArgument.bind(null, target)

describe('addTagsToStream', function() {

  describe('serializations', function() {

    it('should return SerializationException when Tags is not a map', function(done) {
      assertType('Tags', 'Map', done)
    })

    it('should return SerializationException when Tags.a is not a string', function(done) {
      assertType('Tags.a', 'String', done)
    })

    it('should return SerializationException when StreamName is not a String', function(done) {
      assertType('StreamName', 'String', done)
    })

  })

  describe('validations', function() {

    it('should return ValidationException for no StreamName', function(done) {
      assertValidation({}, [
        'Value null at \'tags\' failed to satisfy constraint: ' +
        'Member must not be null',
        'Value null at \'streamName\' failed to satisfy constraint: ' +
        'Member must not be null',
      ], done)
    })

    it('should return ValidationException for empty StreamName', function(done) {
      assertValidation({StreamName: '', Tags: {}}, [
        'Value \'{}\' at \'tags\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+',
        'Value \'\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length greater than or equal to 1',
      ], done)
    })

    it('should return ValidationException for long StreamName', function(done) {
      var name = new Array(129 + 1).join('a')
      assertValidation({StreamName: name, Tags: {'a;b': 'b'}}, [
        'Value \'' + name + '\' at \'streamName\' failed to satisfy constraint: ' +
        'Member must have length less than or equal to 128',
      ], done)
    })

    it('should return ValidationException for long tag key', function(done) {
      var name = new Array(129 + 1).join('a'), tags = {a: '1', b: '2'}
      tags[name] = '3'
      assertValidation({StreamName: randomName(), Tags: tags},
        new RegExp('^1 validation error detected: ' +
          'Value \'{[ab12, =]*' + name + '=3[ab12, =]*}\' at \'tags\' failed to satisfy constraint: ' +
          'Map keys must satisfy constraint: \\[Member must have length less than or equal to 128, ' +
          'Member must have length greater than or equal to 1\\]$'), done)
    })

    it('should return ValidationException for long tag value', function(done) {
      var name = new Array(257 + 1).join('a'), tags = {a: '1', b: '2', c: name}
      assertValidation({StreamName: randomName(), Tags: tags},
        new RegExp('^1 validation error detected: ' +
          'Value \'{[ab12, =]*c=' + name + '[ab12, =]*}\' at \'tags\' failed to satisfy constraint: ' +
          'Map value must satisfy constraint: \\[Member must have length less than or equal to 256, ' +
          'Member must have length greater than or equal to 0\\]$'), done)
    })

    it('should return ValidationException for too many tags', function(done) {
      var tags = {a: '1', b: '2', c: '3', d: '4', e: '5', f: '6', g: '7', h: '8', i: '9', j: '10', k: '11'}
      assertValidation({StreamName: randomName(), Tags: tags},
        new RegExp('^1 validation error detected: ' +
          'Value \'{[a-k0-9, =]+}\' at \'tags\' failed to satisfy constraint: ' +
          'Member must have length less than or equal to 10$'), done)
    })

    it('should return ResourceNotFoundException if stream does not exist', function(done) {
      var name1 = randomName()
      assertNotFound({StreamName: name1, Tags: {a: 'b'}},
        'Stream ' + name1 + ' under account ' + helpers.awsAccountId + ' not found.', done)
    })

    it('should return InvalidArgumentException if ; in tag key', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {'abc;def': '1'}},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if tab in tag key', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {'abc\tdef': '1'}},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if newline in tag key', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {'abc\ndef': '1'}},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if comma in tag key', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {'abc,def': '1'}},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if % in tag key', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {'abc%def': '1'}},
        'Failed to add tags to stream ' + helpers.testStream + ' under account ' + helpers.awsAccountId +
        ' because some tags contained illegal characters. The allowed characters are ' +
        'Unicode letters, white-spaces, \'_\',\',\',\'/\',\'=\',\'+\',\'-\',\'@\'.', done)
    })

    it('should return InvalidArgumentException if ; in tag value', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {a: 'abc;def'}},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if tab in tag value', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {a: 'abc\tdef'}},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if newline in tag value', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {a: 'abc\ndef'}},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if comma in tag value', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {a: 'abc,def'}},
        'Some tags contain invalid characters. Valid characters: ' +
        'Unicode letters, digits, white space, _ . / = + - % @.', done)
    })

    it('should return InvalidArgumentException if % in tag value', function(done) {
      assertInvalidArgument({StreamName: helpers.testStream, Tags: {a: 'abc%def'}},
        'Failed to add tags to stream ' + helpers.testStream + ' under account ' + helpers.awsAccountId +
        ' because some tags contained illegal characters. The allowed characters are ' +
        'Unicode letters, white-spaces, \'_\',\',\',\'/\',\'=\',\'+\',\'-\',\'@\'.', done)
    })

  })

  describe('functionality', function() {

    it('should add and remove tags keys', function(done) {
      request(opts({
        StreamName: helpers.testStream,
        Tags: {a: 'a', 'ü0 _.': 'a', '/=+-@': 'a', b: 'ü0 _./=+-@', c: ''},
      }), function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)

        assertInvalidArgument({
          StreamName: helpers.testStream,
          Tags: {d: '', e: '', f: '', g: '', h: '', i: ''},
        }, 'Failed to add tags to stream ' + helpers.testStream + ' under account ' + helpers.awsAccountId +
            ' because a given stream cannot have more than 10 tags associated with it.', function(err) {
          if (err) return done(err)

          request(helpers.opts('ListTagsForStream', {StreamName: helpers.testStream}), function(err, res) {
            if (err) return done(err)
            res.statusCode.should.equal(200)
            res.body.Tags.should.containEql({Key: 'a', Value: 'a'})
            res.body.Tags.should.containEql({Key: 'ü0 _.', Value: 'a'})
            res.body.Tags.should.containEql({Key: '/=+-@', Value: 'a'})
            res.body.Tags.should.containEql({Key: 'b', Value: 'ü0 _./=+-@'})
            res.body.Tags.should.containEql({Key: 'c', Value: ''})

            request(opts({StreamName: helpers.testStream, Tags: {a: 'b'}}), function(err, res) {
              if (err) return done(err)
              res.statusCode.should.equal(200)

              request(helpers.opts('ListTagsForStream', {StreamName: helpers.testStream}), function(err, res) {
                if (err) return done(err)
                res.statusCode.should.equal(200)
                res.body.Tags.should.containEql({Key: 'a', Value: 'b'})
                res.body.Tags.should.containEql({Key: 'ü0 _.', Value: 'a'})
                res.body.Tags.should.containEql({Key: '/=+-@', Value: 'a'})
                res.body.Tags.should.containEql({Key: 'b', Value: 'ü0 _./=+-@'})
                res.body.Tags.should.containEql({Key: 'c', Value: ''})

                request(helpers.opts('RemoveTagsFromStream', {
                  StreamName: helpers.testStream,
                  TagKeys: ['a', 'ü0 _.', '/=+-@', 'b', 'c'],
                }), done)
              })
            })
          })
        })
      })
    })

  })

})

