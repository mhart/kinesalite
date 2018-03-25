var https = require('https'),
    once = require('once'),
    kinesalite = require('..'),
    request = require('./helpers').request,
    uuidRegex = /[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/

function assertBody(statusCode, contentType, body, done) {
  return function(err, res) {
    if (err) return done(err)
    res.body.should.eql(body)
    res.statusCode.should.equal(statusCode)
    if (contentType != null) {
      res.headers['content-type'].should.equal(contentType)
    } else {
      res.headers.should.not.have.property('content-type')
    }
    if (!Buffer.isBuffer(res.body)) {
      if (typeof res.body != 'string') res.body = JSON.stringify(res.body)
      res.body = Buffer.from(res.body, 'utf8')
    }
    res.headers['content-length'].should.equal(String(res.body.length))
    res.headers['x-amzn-requestid'].should.match(uuidRegex)
    new Buffer(res.headers['x-amz-id-2'], 'base64').length.should.be.within(64, 80)
    done()
  }
}

function assertMissingTokenXml(done) {
  return assertBody(403, null,
    '<MissingAuthenticationTokenException>\n' +
    '  <Message>Missing Authentication Token</Message>\n' +
    '</MissingAuthenticationTokenException>\n', done)
}

function assertInvalidSignatureXml(done) {
  return assertBody(403, null,
    '<InvalidSignatureException>\n  <Message>Found both \'X-Amz-Algorithm\' as a query-string param and \'Authorization\' as HTTP header.</Message>\n</InvalidSignatureException>\n', done)
}

function assertIncompleteQueryXml(done) {
  return assertBody(403, null,
    '<IncompleteSignatureException>\n  <Message>AWS query-string parameters must include \'X-Amz-Algorithm\'. AWS query-string parameters must include \'X-Amz-Credential\'. AWS query-string parameters must include \'X-Amz-Signature\'. AWS query-string parameters must include \'X-Amz-SignedHeaders\'. AWS query-string parameters must include \'X-Amz-Date\'. Re-examine the query-string parameters.</Message>\n</IncompleteSignatureException>\n', done)
}

function assertIncompleteHeaderXml(done) {
  return assertBody(403, null,
    '<IncompleteSignatureException>\n  <Message>Authorization header requires \'Credential\' parameter. Authorization header requires \'Signature\' parameter. Authorization header requires \'SignedHeaders\' parameter. Authorization header requires existence of either a \'X-Amz-Date\' or a \'Date\' header. Authorization=a</Message>\n</IncompleteSignatureException>\n', done)
}

function assertAccessDeniedXml(done) {
  return assertBody(403, null,
    '<AccessDeniedException>\n' +
    '  <Message>Unable to determine service/operation name to be authorized</Message>\n' +
    '</AccessDeniedException>\n', done)
}

function assertUnrecognizedClientXml(service, operation, done) {
  return assertBody(403, null,
    '<UnrecognizedClientException>\n' +
    '  <Message>No authorization strategy was found for service: ' + service + ', operation: ' + operation + '</Message>\n' +
    '</UnrecognizedClientException>\n', done)
}

function assertInternalFailureXml(done) {
  return assertBody(500, null, '<InternalFailure/>\n', done)
}

function assertUnknownOperationXml(done) {
  return assertBody(404, null, '<UnknownOperationException/>\n', done)
}

function assertUnknown(done) {
  return assertBody(400, 'application/x-amz-json-1.1', {__type: 'UnknownOperationException'}, done)
}

function assertUnknownDeprecated(done) {
  return assertBody(200, 'application/json', {
    Output: {__type: 'com.amazon.coral.service#UnknownOperationException', message: null},
    Version: '1.0',
  }, done)
}

function assertUnknownCbor(done) {
  return assertBody(400, 'application/x-amz-cbor-1.1', Buffer.concat([
    Buffer.from('bf66', 'hex'),
    Buffer.from('__type', 'utf8'),
    Buffer.from('7819', 'hex'),
    Buffer.from('UnknownOperationException', 'utf8'),
    Buffer.from('ff', 'hex')
  ]), done)
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

function assertSerializationCbor(done) {
  return assertBody(400, 'application/x-amz-cbor-1.1', Buffer.concat([
    Buffer.from('bf66', 'hex'),
    Buffer.from('__type', 'utf8'),
    Buffer.from('76', 'hex'),
    Buffer.from('SerializationException', 'utf8'),
    Buffer.from('ff', 'hex')
  ]), done)
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

function assertOk(done) {
  return assertBody(200, 'application/x-amz-json-1.1', {HasMoreStreams: false, StreamNames: []}, done)
}

function assertCors(headers, done) {
  if (!done) { done = headers; headers = null }
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
    res.headers.should.not.have.property('content-type')
    res.body.should.eql('')
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
        if (err && err.code == 'HPE_INVALID_CONSTANT') return done()
        if (err) return done(err)
        res.statusCode.should.equal(403)
        done()
      })
    })

    // This hairy beast can be used to ensure the correct order of checks in a request
    // BUT BE WARNED – THIS RUNS 3120 TESTS
    if (false) {
      var paths = ['/whatever', '/?X-Amz-Algorithm']
      var authHeaders = ['', 'a']
      var contentHeaders = ['', 'application/x-amz-json-1.0', 'application/x-amz-json-1.1', 'application/json']
      var originHeaders = ['', 'whatever']
      var targetHeaders = ['', 'whatever', 'Kinesis_20131202.', 'Kinesis_20131202.whatever', 'whatever.ListStreams', 'Kinesis_20131202.ListStreams']
      var bodies = ['', 'hello', 'true', 'null', '{}']
      var noSigns = [true, false]
      var methods = ['OPTIONS', 'GET', 'PUT', 'DELETE', 'POST']
      paths.forEach(function(path) {
        authHeaders.forEach(function(authHeader) {
          contentHeaders.forEach(function(contentHeader) {
            originHeaders.forEach(function(originHeader) {
              targetHeaders.forEach(function(targetHeader) {
                bodies.forEach(function(body) {
                  noSigns.forEach(function(noSign) {
                    methods.forEach(function(method) {
                      if ((authHeader || path == '/?X-Amz-Algorithm') && !noSign) return
                      if (~['OPTIONS', 'GET', 'DELETE'].indexOf(method) && body) return

                      var headers = {}
                      if (authHeader) headers.authorization = authHeader
                      if (contentHeader) headers['content-type'] = contentHeader
                      if (originHeader) headers.origin = originHeader
                      if (targetHeader) headers['x-amz-target'] = targetHeader
                      var opts = {method: method, path: path, headers: headers, body: body, noSign: noSign}

                      it('should return exception with ' + JSON.stringify(opts), function(done) {
                        var returnFn

                        if (method == 'OPTIONS' && originHeader) {
                          returnFn = assertCors
                        } else if (method == 'POST' && ~['application/x-amz-json-1.1', 'application/json'].indexOf(contentHeader)) {
                          if (body && body != '{}') {
                            returnFn = contentHeader == 'application/x-amz-json-1.1' ? assertSerialization : assertSerializationDeprecated
                          } else if (contentHeader == 'application/json') {
                            returnFn = assertUnknownDeprecated
                          } else if (targetHeader != 'Kinesis_20131202.ListStreams') {
                            returnFn = assertUnknown
                          } else if (body != '{}') {
                            returnFn = assertSerialization
                          } else if (path == '/?X-Amz-Algorithm') {
                            returnFn = authHeader ? assertInvalid :
                              assertIncomplete.bind(null, 'AWS query-string parameters must include \'X-Amz-Algorithm\'. AWS query-string parameters must include \'X-Amz-Credential\'. AWS query-string parameters must include \'X-Amz-Signature\'. AWS query-string parameters must include \'X-Amz-SignedHeaders\'. AWS query-string parameters must include \'X-Amz-Date\'. Re-examine the query-string parameters.')
                          } else if (authHeader) {
                            returnFn = assertIncomplete.bind(null, 'Authorization header requires \'Credential\' parameter. Authorization header requires \'Signature\' parameter. Authorization header requires \'SignedHeaders\' parameter. Authorization header requires existence of either a \'X-Amz-Date\' or a \'Date\' header. Authorization=a')
                          } else if (noSign) {
                            returnFn = assertMissing
                          } else {
                            returnFn = assertOk
                          }
                        } else if (path == '/?X-Amz-Algorithm') {
                          returnFn = authHeader ? assertInvalidSignatureXml : assertIncompleteQueryXml
                        } else if (authHeader) {
                          returnFn = assertIncompleteHeaderXml
                        } else if (noSign) {
                          returnFn = assertMissingTokenXml
                        } else if (targetHeader == 'whatever.ListStreams') {
                          returnFn = assertUnrecognizedClientXml.bind(null, 'whatever', 'ListStreams')
                        } else if (targetHeader == 'Kinesis_20131202.whatever') {
                          returnFn = assertInternalFailureXml
                        } else if (targetHeader == 'Kinesis_20131202.ListStreams') {
                          returnFn = assertUnknownOperationXml
                        } else {
                          returnFn = assertAccessDeniedXml
                        }
                        request(opts, returnFn(done))
                      })
                    })
                  })
                })
              })
            })
          })
        })
      })
    }

    it('should return CBOR UnknownOperationException if POST with no auth', function(done) {
      request({noSign: true}, assertUnknownCbor(done))
    })

    it('should return AccessDeniedException if GET', function(done) {
      request({method: 'GET'}, assertAccessDeniedXml(done))
    })

    it('should return AccessDeniedException if PUT', function(done) {
      request({method: 'PUT'}, assertAccessDeniedXml(done))
    })

    it('should return AccessDeniedException if DELETE', function(done) {
      request({method: 'DELETE'}, assertAccessDeniedXml(done))
    })

    it('should return CBOR UnknownOperationException if POST with no body', function(done) {
      request(assertUnknownCbor(done))
    })

    it('should return AccessDeniedException if body and no Content-Type', function(done) {
      request({body: '{}'}, assertAccessDeniedXml(done))
    })

    it('should return CBOR UnknownOperationException if x-amz-json-1.0 Content-Type', function(done) {
      request({headers: {'content-type': 'application/x-amz-json-1.0'}}, assertUnknownCbor(done))
    })

    it('should return CBOR UnknownOperationException if random Content-Type', function(done) {
      request({headers: {'content-type': 'application/x-amz-json-1.1asdf'}}, assertUnknownCbor(done))
    })

    it('should return CBOR UnknownOperationException if random target', function(done) {
      request({headers: {'x-amz-target': 'Whatever'}}, assertUnknownCbor(done))
    })

    it('should return CBOR UnknownOperationException if real service with empty action', function(done) {
      request({headers: {'x-amz-target': 'Kinesis_20131202.'}}, assertUnknownCbor(done))
    })

    it('should return CBOR UnknownOperationException if random action', function(done) {
      request({headers: {'x-amz-target': 'Kinesis_20131202.Whatever'}}, assertUnknownCbor(done))
    })

    it('should return CBOR UnknownOperationException if random service with random action', function(done) {
      request({headers: {'x-amz-target': 'Whatever.Whatever'}}, assertUnknownCbor(done))
    })

    it('should return CBOR UnknownOperationException if incomplete action', function(done) {
      request({headers: {'x-amz-target': 'Kinesis_20131202.ListStream'}}, assertUnknownCbor(done))
    })

    it('should return CBOR SerializationException if no Content-Type', function(done) {
      request({headers: {'x-amz-target': 'Kinesis_20131202.ListStreams'}}, assertSerializationCbor(done))
    })

    it('should return CBOR UnknownOperationException and set CORS if using Origin', function(done) {
      request({headers: {origin: 'whatever'}}, function(err, res) {
        if (err) return done(err)
        res.headers['access-control-allow-origin'].should.equal('*')
        if (res.rawHeaders) {
          res.headers['access-control-expose-headers'].should.equal('x-amzn-RequestId,x-amzn-ErrorType,x-amz-request-id,x-amz-id-2,x-amzn-ErrorMessage,Date')
        } else {
          res.headers['access-control-expose-headers'].should.equal('x-amz-request-id')
        }
        assertUnknownCbor(done)(err, res)
      })
    })

    it('should set CORS if OPTIONS and Origin and no auth', function(done) {
      request({method: 'OPTIONS', headers: {origin: 'whatever'}, noSign: true}, assertCors(null, done))
    })

    it('should set CORS if OPTIONS and Origin and auth', function(done) {
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

        https.request({host: '127.0.0.1', port: port, rejectUnauthorized: false}, function(res) {
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

    it('should return UnknownOperationException if no target', function(done) {
      request({headers: {'content-type': 'application/x-amz-json-1.1'}}, assertUnknown(done))
    })

    it('should return UnknownOperationException if no target and no auth', function(done) {
      request({headers: {'content-type': 'application/x-amz-json-1.1'}, noSign: true}, assertUnknown(done))
    })

    it('should return CBOR UnknownOperationException if no target and application/json', function(done) {
      request({headers: {'content-type': 'application/json'}}, assertUnknownCbor(done))
    })

    it('should return CBOR SerializationException if valid target and application/json', function(done) {
      request({headers: {
        'content-type': 'application/json',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }}, assertSerializationCbor(done))
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

    it('should return SerializationException if non-JSON body and application/json with spaces', function(done) {
      request({headers: {
        'content-type': '     application/json     ;',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }, body: 'hello', noSign: true}, assertSerializationDeprecated(done))
    })

    it('should return MissingAuthenticationTokenException if no auth', function(done) {
      request({headers: {
        'content-type': 'application/x-amz-json-1.1',
        'x-amz-target': 'Kinesis_20131202.ListStreams',
      }, body: '{}', noSign: true}, assertMissing(done))
    })

    it('should return MissingAuthenticationTokenException if no auth and content type with spaces', function(done) {
      request({headers: {
        'content-type': '   application/x-amz-json-1.1   ;whatever',
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
          'Authorization': 'X',
        },
        body: '{}',
        noSign: true,
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
        noSign: true,
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
        noSign: true,
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
