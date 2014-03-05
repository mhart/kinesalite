exports.checkTypes = checkTypes
exports.checkValidations = checkValidations
exports.toLowerFirst = toLowerFirst

function checkTypes(data, types) {
  var key
  for (key in data) {
    // TODO: deal with nulls
    if (!types[key] || data[key] == null)
      delete data[key]
  }

  return Object.keys(types).reduce(function(newData, key) {
    var val = checkType(data[key], types[key])
    if (val != null) newData[key] = val
    return newData
  }, {})

  function typeError(msg) {
    var err = new Error(msg)
    err.statusCode = 400
    err.body = {
      __type: 'SerializationException',
      Message: msg,
    }
    return err
  }

  function classForNumber(val) {
    return val % 1 !== 0 ? 'java.lang.Double' :
      val >= -32768 && val <= 32767 ? 'java.lang.Short' :
      val >= -2147483648 && val <= 2147483647 ? 'java.lang.Integer' : 'java.lang.Long'
  }

  function checkType(val, type) {
    if (val == null) return
    var actualType = type.type || type
    switch (actualType) {
      case 'Boolean':
        switch (typeof val) {
          case 'number':
            // Strangely floats seem to be fine...?
            var numClass = classForNumber(val)
            if (numClass != 'java.lang.Double')
              throw typeError('class ' + numClass + ' can not be converted to an Boolean')
            val = Math.abs(val) >= 1
            break
          case 'string':
            //"\'HELLOWTF\' can not be converted to an Boolean"
            // seems to convert to uppercase
            // 'true'/'false'/'1'/'0'/'no'/'yes' seem to convert fine
            val = val.toUpperCase()
            throw typeError('\'' + val + '\' can not be converted to an Boolean')
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
            throw typeError('Start of structure or map found where not expected.')
        }
        return val
      case 'Short':
      case 'Integer':
      case 'Long':
      case 'Double':
        switch (typeof val) {
          case 'boolean':
            throw typeError('class java.lang.Boolean can not be converted to an ' + actualType)
          case 'number':
            if (actualType != 'Double') val = Math.floor(val)
            break
          case 'string':
            throw typeError('class java.lang.String can not be converted to an ' + actualType)
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
            throw typeError('Start of structure or map found where not expected.')
        }
        return val
      case 'String':
        switch (typeof val) {
          case 'boolean':
            throw typeError('class java.lang.Boolean can not be converted to an String')
          case 'number':
            throw typeError('class ' + classForNumber(val) + ' can not be converted to an String')
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
            throw typeError('Start of structure or map found where not expected.')
        }
        return val
      case 'Blob':
        switch (typeof val) {
          case 'boolean':
            throw typeError('class java.lang.Boolean can not be converted to a Blob')
          case 'number':
            throw typeError('class ' + classForNumber(val) + ' can not be converted to a Blob')
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
            throw typeError('Start of structure or map found where not expected.')
        }
        if (val.length % 4)
          throw typeError('\'' + val + '\' can not be converted to a Blob: ' +
            'Base64 encoded length is expected a multiple of 4 bytes but found: ' + val.length)
        var match = val.match(/[^a-zA-Z0-9+/=]|\=[^=]/)
        if (match)
          throw typeError('\'' + val + '\' can not be converted to a Blob: ' +
            'Invalid Base64 character: \'' + match[0][0] + '\'')
        // TODO: need a better check than this...
        if (new Buffer(val, 'base64').toString('base64') != val)
          throw typeError('\'' + val + '\' can not be converted to a Blob: ' +
            'Invalid last non-pad Base64 character dectected')
        return val
      case 'List':
        switch (typeof val) {
          case 'boolean':
          case 'number':
          case 'string':
            throw typeError('Expected list or null')
          case 'object':
            if (!Array.isArray(val)) throw typeError('Start of structure or map found where not expected.')
        }
        return val.map(function(child) { return checkType(child, type.children) })
      case 'Map':
        switch (typeof val) {
          case 'boolean':
          case 'number':
          case 'string':
            throw typeError('Expected map or null')
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
        }
        return Object.keys(val).reduce(function(newVal, key) {
          newVal[key] = checkType(val[key], type.children)
          return newVal
        }, {})
      case 'Structure':
        switch (typeof val) {
          case 'boolean':
          case 'number':
          case 'string':
            throw typeError('Expected null')
          case 'object':
            if (Array.isArray(val)) throw typeError('Start of list found where not expected')
        }
        return checkTypes(val, type.children)
      default:
        throw new Error('Unknown type: ' + actualType)
    }
  }
}

function checkValidations(data, validations, custom, target) {
  var attr, msg, errors = []
  function validationError(msg) {
    var err = new Error(msg)
    err.statusCode = 400
    err.body = {
      __type: 'ValidationException',
      message: msg,
    }
    return err
  }

  for (attr in validations) {
    if (validations[attr].required && data[attr] == null) {
      throw validationError('The paramater \'' + toLowerFirst(attr) + '\' is required but was not present in the request')
    }
    if (validations[attr].requiredMap && (data[attr] == null || !Object.keys(data[attr]).length)) {
      throw validationError('The ' + toLowerFirst(attr) + ' parameter is required for ' + target)
    }
    if (validations[attr].streamName) {
      msg = validateStreamName(attr, data[attr])
      if (msg) throw validationError(msg)
    }
  }

  function checkNonRequireds(data, types, parent) {
    for (var attr in types) {
      checkNonRequired(attr, data[attr], types[attr], parent)
    }
  }

  checkNonRequireds(data, validations)

  function checkNonRequired(attr, data, validations, parent) {
    if (validations == null || typeof validations != 'object') return
    for (var validation in validations) {
      if (errors.length >= 10) return
      if (~['type', 'required', 'requiredMap', 'streamName'].indexOf(validation)) continue
      if (validation != 'notNull' && data == null) continue
      if (validation == 'children') {
        if (validations.type == 'List') {
          for (var i = 0; i < data.length; i++) {
            checkNonRequired('member', data[i], validations.children,
              (parent ? parent + '.' : '') + toLowerFirst(attr) + '.' + (i + 1))
          }
          continue
        } else if (validations.type == 'Map') {
          // TODO: Always reverse?
          Object.keys(data).reverse().forEach(function(key) {
            checkNonRequired('member', data[key], validations.children,
              (parent ? parent + '.' : '') + toLowerFirst(attr) + '.' + key)
          })
          continue
        }
        checkNonRequireds(data, validations.children, (parent ? parent + '.' : '') + toLowerFirst(attr))
        continue
      }
      validateFns[validation](parent, attr, validations[validation], data, errors)
    }
  }

  if (errors.length)
    throw validationError(errors.length + ' validation error' + (errors.length > 1 ? 's' : '') + ' detected: ' + errors.join('; '))

  if (custom) {
    msg = custom(data)
    if (msg) throw validationError(msg)
  }
}

var validateFns = {}
validateFns.notNull = function(parent, key, val, data, errors) {
  validate(data != null, 'Member must not be null', data, parent, key, errors)
}
validateFns.greaterThanOrEqual = function(parent, key, val, data, errors) {
  validate(data >= val, 'Member must have value greater than or equal to ' + val, data, parent, key, errors)
}
validateFns.lessThanOrEqual = function(parent, key, val, data, errors) {
  validate(data <= val, 'Member must have value less than or equal to ' + val, data, parent, key, errors)
}
validateFns.regex = function(parent, key, pattern, data, errors) {
  validate(RegExp('^' + pattern + '$').test(data), 'Member must satisfy regular expression pattern: ' + pattern, data, parent, key, errors)
}
validateFns.lengthGreaterThanOrEqual = function(parent, key, val, data, errors) {
  var length = (typeof data == 'object' && !Array.isArray(data)) ? Object.keys(data).length : data.length
  validate(length >= val, 'Member must have length greater than or equal to ' + val, data, parent, key, errors)
}
validateFns.lengthLessThanOrEqual = function(parent, key, val, data, errors) {
  var length = (typeof data == 'object' && !Array.isArray(data)) ? Object.keys(data).length : data.length
  validate(length <= val, 'Member must have length less than or equal to ' + val, data, parent, key, errors)
}
validateFns.enum = function(parent, key, val, data, errors) {
  validate(~val.indexOf(data), 'Member must satisfy enum value set: [' + val.join(', ') + ']', data, parent, key, errors)
}

function validate(predicate, msg, data, parent, key, errors) {
  if (predicate) return
  var value = valueStr(data)
  if (value != 'null') value = '\'' + value + '\''
  parent = parent ? parent + '.' : ''
  errors.push('Value ' + value + ' at \'' + parent + toLowerFirst(key) + '\' failed to satisfy constraint: ' + msg)
}

function validateStreamName(key, val) {
  if (val == null) return
  if (val.length < 1 || val.length > 128)
    return key + ' must be at least 1 characters long and at most 128 characters long'
}

function toLowerFirst(str) {
  return str[0].toLowerCase() + str.slice(1)
}

function valueStr(data) {
  return data == null ? 'null' : Array.isArray(data) ? '[' + data.join(', ') + ']' : typeof data == 'object' ? JSON.stringify(data) : data
}

