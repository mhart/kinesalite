var https = require('https'),
    once = require('once'),
    kinesalite = require('..'),
    request = require('./helpers').request,
    uuidRegex = /[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/

function assertBody(statusCode, contentType, body, done) {
  return function(err, res) {
    if (err) return done(err)
    res.statusCode.should.equal(statusCode)
    res.body.should.eql(body)
    if (contentType != null) {
      res.headers['content-type'].should.equal(contentType)
    } else {
      res.headers.should.not.have.property('content-type')
    }
    if (typeof res.body != 'string') res.body = JSON.stringify(res.body)
    res.headers['content-length'].should.equal(String(Buffer.byteLength(res.body, 'utf8')))
    res.headers['x-amzn-requestid'].should.match(uuidRegex)
    new Buffer(res.headers['x-amz-id-2'], 'base64').length.should.be.within(72, 80)
    done()
  }
}

describe('kinesalite connections', function() {

  describe('basic', function() {

    it.skip('should return 413 if request too large', function(done) {
      this.timeout(100000)
      var body = Array(7 * 1024 * 1024 + 1), i
      for (i = 0; i < body.length; i++)
        body[i] = 'a'

      request({body: body.join(''), noSign: true}, function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(413)
        res.headers['transfer-encoding'].should.equal('chunked')
        done()
      })
    })

    it.skip('should not return 413 if request not too large', function(done) {
      this.timeout(100000)
      var body = Array(7 * 1024 * 1024), i
      for (i = 0; i < body.length; i++)
        body[i] = 'a'

      request({body: body.join(''), noSign: true}, function(err, res) {
        if (err && err.code == 'HPE_INVALID_CONSTANT') return
        if (err) return done(err)
        res.statusCode.should.equal(403)
        done()
      })
    })

    it('should hang up socket if a DELETE', function(done) {
      request({method: 'DELETE', noSign: true}, function(err) {
        err.code.should.equal('ECONNRESET')
        done()
      })
    })

    function assertMissingTokenXml(done) {
      return assertBody(403, null,
        '<MissingAuthenticationTokenException>\n' +
        '  <Message>Missing Authentication Token</Message>\n' +
        '</MissingAuthenticationTokenException>\n', done)
    }

    function assertAccessDeniedXml(done) {
      return assertBody(403, null,
        '<AccessDeniedException>\n' +
        '  <Message>Unable to determine service/operation name to be authorized</Message>\n' +
        '</AccessDeniedException>\n', done)
    }

    it('should return MissingAuthenticationTokenException if OPTIONS with no auth', function(done) {
      request({method: 'OPTIONS', noSign: true}, assertMissingTokenXml(done))
    })

    it('should return MissingAuthenticationTokenException if GET with no auth', function(done) {
      request({method: 'GET', noSign: true}, assertMissingTokenXml(done))
    })

    it('should return MissingAuthenticationTokenException if PUT with no auth', function(done) {
      request({method: 'PUT', noSign: true}, assertMissingTokenXml(done))
    })

    it('should return MissingAuthenticationTokenException if POST with no auth', function(done) {
      request({noSign: true}, assertMissingTokenXml(done))
    })

    it('should return AccessDeniedException if GET', function(done) {
      request({method: 'GET'}, assertAccessDeniedXml(done))
    })

    it('should return AccessDeniedException if PUT', function(done) {
      request({method: 'PUT'}, assertAccessDeniedXml(done))
    })

    it('should return AccessDeniedException if POST with no body', function(done) {
      request(assertAccessDeniedXml(done))
    })

    it('should return AccessDeniedException if body and no Content-Type', function(done) {
      request({body: '{}'}, assertAccessDeniedXml(done))
    })

    it('should return AccessDeniedException if x-amz-json-1.0 Content-Type', function(done) {
      request({headers: {'content-type': 'application/x-amz-json-1.0'}}, assertAccessDeniedXml(done))
    })

    it('should return AccessDeniedException if invalid target', function(done) {
      request({headers: {'x-amz-target': 'Kinesis_20131202.ListStream'}}, assertAccessDeniedXml(done))
    })

    it('should return AccessDeniedException if no Content-Type', function(done) {
      request({headers: {'x-amz-target': 'Kinesis_20131202.ListStreams'}}, assertAccessDeniedXml(done))
    })

    it('should return AccessDeniedException and set CORS if using Origin', function(done) {
      request({headers: {origin: 'whatever'}}, function (err, res) {
        if (err) return done(err)
        res.headers['access-control-allow-origin'].should.equal('*')
        if (res.rawHeaders) {
          res.headers['access-control-expose-headers'].should.equal('x-amz-request-id, x-amz-id-2')
        } else {
          res.headers['access-control-expose-headers'].should.equal('x-amz-request-id')
        }
        assertAccessDeniedXml(done)(err, res)
      })
    })

    function assertCors(headers, done) {
      return function(err, res) {
        if (err) return done(err)
        res.statusCode.should.equal(200)
        res.headers['x-amzn-requestid'].should.match(uuidRegex)
        res.headers['access-control-allow-origin'].should.equal('*')
        Object.keys(headers || {}).forEach(function(header) {
          res.headers[header].should.equal(headers[header])
        })
        res.headers['access-control-max-age'].should.equal('172800')
        res.headers['content-length'].should.equal('0')
        res.headers.should.not.have.property('x-amz-id-2')
        res.body.should.eql('')
        done()
      }
    }

    it('should set CORS if OPTIONS and Origin', function(done) {
      request({method: 'OPTIONS', headers: {origin: 'whatever'}}, assertCors(null, done))
    })

    it('should set CORS if OPTIONS and Origin and Headers', function(done) {
      request({method: 'OPTIONS', headers: {
        origin: 'whatever',
        'access-control-request-headers': 'a, b, c',
      }}, assertCors({
        'access-control-allow-headers': 'a, b, c',
      }, done))
    })

    it('should set CORS if OPTIONS and Origin and Headers and Method', function(done) {
      request({method: 'OPTIONS', headers: {
        origin: 'whatever',
        'access-control-request-headers': 'a, b, c',
        'access-control-request-method': 'd',
      }}, assertCors({
        'access-control-allow-headers': 'a, b, c',
        'access-control-allow-methods': 'd',
      }, done))
    })

    it('should connect to SSL', function(done) {
      var port = 10000 + Math.round(Math.random() * 10000), kinesaliteServer = kinesalite({ssl: true})

      kinesaliteServer.listen(port, function(err) {
        if (err) return done(err)

        done = once(done)

        https.request({host: 'localhost', port: port, rejectUnauthorized : false}, function(res) {
          res.on('error', done)
          res.on('data', function() {})
          res.on('end', function() {
            res.statusCode.should.equal(403)
            kinesaliteServer.close(done)
          })
        }).on('error', done).end()
      })
    })

  })

  describe('JSON', function() {

    function assertUnknown(done) {
      return assertBody(400, 'application/x-amz-json-1.1', {__type: 'UnknownOperationException'}, done)
    }

    function assertUnknownDeprecated(done) {
      return assertBody(200, 'application/json', {
        Output: {__type: 'com.amazon.coral.service#UnknownOperationException', message: null},
        Version: '1.0',
      }, done)
    }

    function assertSerialization(done) {
      return assertBody(400, 'application/x-amz-json-1.1', {__type: 'SerializationException'}, done)
    }

    function assertSerializationDeprecated(done) {
      return assertBody(200, 'application/json', {
        Output: {__type: 'com.amazon.coral.service#SerializationException', Message: null},
        Version: '1.0',
      }, done)
    }

    function assertMissing(done) {
      return assertBody(400, 'application/x-amz-json-1.1', {
        __type: 'MissingAuthenticationTokenException',
        message: 'Missing Authentication Token',
      }, done)
    }

    function assertIncomplete(msg, done) {
      return assertBody(400, 'application/x-amz-json-1.1', {
        __type: 'IncompleteSignatureException',
        message: msg,
      }, done)
    }

    function assertInvalid(done) {
      return assertBody(400, 'application/x-amz-json-1.1', {
        __type: 'InvalidSignatureException',
        message: 'Found both \'X-Amz-Algorithm\' as a query-string param and \'Authorization\' as HTTP header.',
      }, done)
    }

    it('should return UnknownOperationException if no target', function(done) {
      request({headers: {'content-type': 'application/x-amz-json-1.1'}}, assertUnknown(done))
    })

    it('should return UnknownOperationException if no target and no auth', function(done) {
      request({headers: {'content-type': 'application/x-amz-json-1.1'}, noSign: true}, assertUnknown(done))
    })

    it('should return UnknownOperationException if no target and application/json', function(done) {
      request({headers: {'content-type': 'application/json'}}, assertUnknownDeprecated(done))
    })

    it('should return UnknownOperationException if valid target and application/json', function(done) {
      request({headers: {
        'content-type': 'application/json',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }}, assertUnknownDeprecated(done))
    })

    it('should return SerializationException if no body', function(done) {
      request({headers: {
        'content-type': 'application/x-amz-json-1.1',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }}, assertSerialization(done))
    })

    it('should return SerializationException if no body and no auth', function(done) {
      request({headers: {
        'content-type': 'application/x-amz-json-1.1',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }, noSign: true}, assertSerialization(done))
    })

    it('should return SerializationException if non-JSON body', function(done) {
      request({headers: {
        'content-type': 'application/x-amz-json-1.1',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }, body: 'hello', noSign: true}, assertSerialization(done))
    })

    it('should return UnknownOperationException if valid target and body and application/json', function(done) {
      request({headers: {
        'content-type': 'application/json',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }, body: '{}', noSign: true}, assertUnknownDeprecated(done))
    })

    it('should return SerializationException if non-JSON body and application/json', function(done) {
      request({headers: {
        'content-type': 'application/json',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }, body: 'hello', noSign: true}, assertSerializationDeprecated(done))
    })

    it('should return MissingAuthenticationTokenException if no auth', function(done) {
      request({headers: {
        'content-type': 'application/x-amz-json-1.1',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }, body: '{}', noSign: true}, assertMissing(done))
    })

    it('should return IncompleteSignatureException if invalid auth', function(done) {
      request({headers: {
        'content-type': 'application/x-amz-json-1.1',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
        'Authorization': 'X',
      }, body: '{}', noSign: true},
        assertIncomplete('Authorization header requires \'Credential\' parameter. ' +
          'Authorization header requires \'Signature\' parameter. ' +
          'Authorization header requires \'SignedHeaders\' parameter. ' +
          'Authorization header requires existence of either a \'X-Amz-Date\' or a \'Date\' header. ' +
          'Authorization=X', done))
    })

    it('should return IncompleteSignatureException if incomplete auth header and query', function(done) {
      request({
        path: '/?X-Amz-Algorith',
        headers: {
          'content-type': 'application/x-amz-json-1.1',
          'x-amz-target': 'Kinesis_20131202.ListStreams',
          'Authorization': 'X'
        },
        body: '{}',
        noSign: true
      }, assertIncomplete('Authorization header requires \'Credential\' parameter. ' +
        'Authorization header requires \'Signature\' parameter. ' +
        'Authorization header requires \'SignedHeaders\' parameter. ' +
        'Authorization header requires existence of either a \'X-Amz-Date\' or a \'Date\' header. ' +
        'Authorization=X', done))
    })

    it('should return MissingAuthenticationTokenException if all query params except X-Amz-Algorithm', function(done) {
      request({
        path: '/?X-Amz-Credential=a&X-Amz-Signature=b&X-Amz-SignedHeaders=c&X-Amz-Date=d',
        headers: {
          'content-type': 'application/x-amz-json-1.1',
          'x-amz-target': 'Kinesis_20131202.ListStreams',
        },
        body: '{}',
        noSign: true
      }, assertMissing(done))
    })

    it('should return InvalidSignatureException if both auth header and query', function(done) {
      request({
        path: '/?X-Amz-Algorithm',
        headers: {
          'content-type': 'application/x-amz-json-1.1',
          'x-amz-target': 'Kinesis_20131202.ListStreams',
          'Authorization': 'X',
        },
        body: '{}',
        noSign: true
      }, assertInvalid(done))
    })

    it('should return IncompleteSignatureException if header is "AWS4- Signature=b Credential=a"', function(done) {
      request({
        headers: {
          'content-type': 'application/x-amz-json-1.1',
          'x-amz-target': 'Kinesis_20131202.ListStreams',
          'Authorization': 'AWS4- Signature=b Credential=a',
          'Date': 'a',
        },
        body: '{}',
        noSign: true,
      }, assertIncomplete('Authorization header requires \'SignedHeaders\' parameter. ' +
        'Authorization=AWS4- Signature=b Credential=a', done))
    })

    it('should return IncompleteSignatureException if header is "AWS4- Signature=b,Credential=a"', function(done) {
      request({
        headers: {
          'content-type': 'application/x-amz-json-1.1',
          'x-amz-target': 'Kinesis_20131202.ListStreams',
          'Authorization': 'AWS4- Signature=b,Credential=a',
          'Date': 'a',
        },
        body: '{}',
        noSign: true,
      }, assertIncomplete('Authorization header requires \'SignedHeaders\' parameter. ' +
        'Authorization=AWS4- Signature=b,Credential=a', done))
    })

    it('should return IncompleteSignatureException if header is "AWS4- Signature=b, Credential=a"', function(done) {
      request({
        headers: {
          'content-type': 'application/x-amz-json-1.1',
          'x-amz-target': 'Kinesis_20131202.ListStreams',
          'Authorization': 'AWS4- Signature=b, Credential=a',
          'Date': 'a',
        },
        body: '{}',
        noSign: true,
      }, assertIncomplete('Authorization header requires \'SignedHeaders\' parameter. ' +
        'Authorization=AWS4- Signature=b, Credential=a', done))
    })

    it('should return IncompleteSignatureException if empty X-Amz-Algorithm query', function(done) {
      request({
        path: '/?X-Amz-Algorithm',
        headers: {
          'content-type': 'application/x-amz-json-1.1',
          'x-amz-target': 'Kinesis_20131202.ListStreams',
        },
        body: '{}',
        noSign: true,
      }, assertIncomplete('AWS query-string parameters must include \'X-Amz-Algorithm\'. ' +
        'AWS query-string parameters must include \'X-Amz-Credential\'. ' +
        'AWS query-string parameters must include \'X-Amz-Signature\'. ' +
        'AWS query-string parameters must include \'X-Amz-SignedHeaders\'. ' +
        'AWS query-string parameters must include \'X-Amz-Date\'. ' +
        'Re-examine the query-string parameters.', done))
    })

    it('should return IncompleteSignatureException if missing X-Amz-SignedHeaders query', function(done) {
      request({
        path: '/?X-Amz-Algorithm=a&X-Amz-Credential=b&X-Amz-Signature=c&X-Amz-Date=d',
        headers: {
          'content-type': 'application/x-amz-json-1.1',
          'x-amz-target': 'Kinesis_20131202.ListStreams',
        },
        body: '{}',
        noSign: true,
      }, assertIncomplete('AWS query-string parameters must include \'X-Amz-SignedHeaders\'. ' +
        'Re-examine the query-string parameters.', done))
    })

  })

})
