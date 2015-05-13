var https = require('https'),
    http = require('http'),
    fs = require('fs'),
    path = require('path'),
    url = require('url'),
    crypto = require('crypto'),
    uuid = require('node-uuid'),
    validations = require('./validations'),
    db = require('./db')

var MAX_REQUEST_BYTES = 7 * 1024 * 1024

var validApis = ['Kinesis_20131202'],
    validOperations = ['AddTagsToStream', 'CreateStream', 'DeleteStream', 'DescribeStream', 'GetRecords',
      'GetShardIterator', 'ListStreams', 'ListTagsForStream', 'MergeShards', 'PutRecord', 'PutRecords',
      'RemoveTagsFromStream', 'SplitShard'],
    actions = {},
    actionValidations = {}

module.exports = kinesalite

function kinesalite(options) {
  options = options || {}
  var server, store = db.create(options), requestHandler = httpHandler.bind(null, store)

  if (options.ssl) {
    options.key = options.key || fs.readFileSync(path.join(__dirname, 'key.pem'))
    options.cert = options.cert || fs.readFileSync(path.join(__dirname, 'cert.pem'))
    options.ca = options.ca || fs.readFileSync(path.join(__dirname, 'ca.pem'))
    server = https.createServer(options, requestHandler)
  } else {
    server = http.createServer(requestHandler)
  }

  // Ensure we close DB when we're closing the server too
  var httpServerClose = server.close, httpServerListen = server.listen
  server.close = function(cb) {
    store.db.close(function(err) {
      if (err) return cb(err)
      // Recreate the store if the user wants to listen again
      server.listen = function() {
        store.recreate()
        httpServerListen.apply(server, arguments)
      }
      httpServerClose.call(server, cb)
    })
  }

  return server
}

validOperations.forEach(function(action) {
  action = validations.toLowerFirst(action)
  actions[action] = require('./actions/' + action)
  actionValidations[action] = require('./validations/' + action)
})

function sendData(req, res, data, statusCode) {
  var body = data != null ? JSON.stringify(data) : ''
  req.removeAllListeners()
  res.statusCode = statusCode || 200
  res.setHeader('Content-Type', res.contentType)
  res.setHeader('Content-Length', Buffer.byteLength(body, 'utf8'))
  // AWS doesn't send a 'Connection' header but seems to use keep-alive behaviour
  // res.setHeader('Connection', '')
  // res.shouldKeepAlive = false
  res.end(body)
}

function httpHandler(store, req, res) {
  if (req.method == 'DELETE') {
    req.destroy()
    return res.destroy()
  }
  var body
  req.on('error', function(err) { throw err })
  req.on('data', function(data) {
    var newLength = data.length + (body ? body.length : 0)
    if (newLength > MAX_REQUEST_BYTES) {
      req.removeAllListeners()
      res.statusCode = 413
      res.setHeader('Transfer-Encoding', 'chunked')
      return res.end()
    }
    body = body ? Buffer.concat([body, data], newLength) : data
  })
  req.on('end', function() {

    body = body ? body.toString() : ''

    // All responses after this point have a RequestId
    res.setHeader('x-amzn-RequestId', uuid.v1())
    if (req.method != 'OPTIONS' || !req.headers.origin)
      res.setHeader('x-amz-id-2', crypto.randomBytes(72).toString('base64'))

    if (req.headers.origin) {
      res.setHeader('Access-Control-Allow-Origin', '*')

      if (req.method == 'OPTIONS') {
        if (req.headers['access-control-request-headers'])
          res.setHeader('Access-Control-Allow-Headers', req.headers['access-control-request-headers'])

        if (req.headers['access-control-request-method'])
          res.setHeader('Access-Control-Allow-Methods', req.headers['access-control-request-method'])

        res.setHeader('Access-Control-Max-Age', 172800)
        res.setHeader('Content-Length', 0)
        req.removeAllListeners()
        return res.end()
      } else {
        res.setHeader('Access-Control-Expose-Headers', ['x-amz-request-id', 'x-amz-id-2'])
      }
    }

    var contentType = req.headers['content-type']

    if ((req.method != 'OPTIONS' && contentType != 'application/x-amz-json-1.1' && contentType != 'application/json') ||
        (req.method == 'OPTIONS' && !req.headers.origin)) {
      req.removeAllListeners()
      res.statusCode = 403
      body = req.headers.authorization ?
          '<AccessDeniedException>\n' +
          '  <Message>Unable to determine service/operation name to be authorized</Message>\n' +
          '</AccessDeniedException>\n' :
          '<MissingAuthenticationTokenException>\n' +
          '  <Message>Missing Authentication Token</Message>\n' +
          '</MissingAuthenticationTokenException>\n'
      res.setHeader('Content-Length', Buffer.byteLength(body, 'utf8'))
      return res.end(body)
    }

    res.contentType = contentType

    var target = (req.headers['x-amz-target'] || '').split('.')

    if (target.length != 2 || !~validApis.indexOf(target[0]) || !~validOperations.indexOf(target[1])) {
      if (contentType == 'application/json') {
        return sendData(req, res, {
          Output: {__type: 'com.amazon.coral.service#UnknownOperationException', message: null},
          Version: '1.0',
        }, 200)
      }
      return sendData(req, res, {__type: 'UnknownOperationException'}, 400)
    }

    // AWS doesn't seem to care about the HTTP path, so no checking needed for that

    var action = validations.toLowerFirst(target[1])

    // THEN check body, see if the JSON parses:

    var data
    if (contentType != 'application/json' || (contentType == 'application/json' && body)) {
      try {
        data = JSON.parse(body)
      } catch (e) {
        if (contentType == 'application/json') {
          return sendData(req, res, {
            Output: {__type: 'com.amazon.coral.service#SerializationException', Message: null},
            Version: '1.0',
          }, 200)
        }
        return sendData(req, res, {__type: 'SerializationException'}, 400)
      }
    }

    // After this point, application/json doesn't seem to progress any further
    if (contentType == 'application/json') {
      return sendData(req, res, {
        Output: {__type: 'com.amazon.coral.service#UnknownOperationException', message: null},
        Version: '1.0',
      }, 200)
    }

    var authHeader = req.headers.authorization
    var query = url.parse(req.url, true).query
    var authQuery = 'X-Amz-Algorithm' in query

    if (authHeader && authQuery)
      return sendData(req, res, {
        __type: 'InvalidSignatureException',
        message: 'Found both \'X-Amz-Algorithm\' as a query-string param and \'Authorization\' as HTTP header.',
      }, 400)

    if (!authHeader && !authQuery)
      return sendData(req, res, {
        __type: 'MissingAuthenticationTokenException',
        message: 'Missing Authentication Token',
      }, 400)

    var msg = '', params

    if (authHeader) {
      params = ['Credential', 'Signature', 'SignedHeaders']
      var authParams = authHeader.split(/,| /).slice(1).filter(Boolean).reduce(function(obj, x) {
        var keyVal = x.trim().split('=')
        obj[keyVal[0]] = keyVal[1]
        return obj
      }, {})
      params.forEach(function(param) {
        if (!authParams[param])
          msg += 'Authorization header requires \'' + param + '\' parameter. '
      })
      if (!req.headers['x-amz-date'] && !req.headers.date)
        msg += 'Authorization header requires existence of either a \'X-Amz-Date\' or a \'Date\' header. '
      if (msg) msg += 'Authorization=' + authHeader

    } else {
      params = ['X-Amz-Algorithm', 'X-Amz-Credential', 'X-Amz-Signature', 'X-Amz-SignedHeaders', 'X-Amz-Date']
      params.forEach(function(param) {
        if (!query[param])
          msg += 'AWS query-string parameters must include \'' + param + '\'. '
      })
      if (msg) msg += 'Re-examine the query-string parameters.'
    }

    if (msg) {
      return sendData(req, res, {
        __type: 'IncompleteSignatureException',
        message: msg,
      }, 400)
    }

    var actionValidation = actionValidations[action]
    try {
      data = validations.checkTypes(data, actionValidation.types)
      validations.checkValidations(data, actionValidation.types, actionValidation.custom, target[1])
    } catch (e) {
      if (e.statusCode) return sendData(req, res, e.body, e.statusCode)
      throw e
    }

    actions[action](store, data, function(err, data) {
      if (err && err.statusCode) return sendData(req, res, err.body, err.statusCode)
      if (err) throw err
      sendData(req, res, data)
    })
  })
}

if (require.main === module) kinesalite().listen(4567)

