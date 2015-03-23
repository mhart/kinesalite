exports.types = {
  Tags: {
    type: 'Map',
    notNull: true,
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 10,
    childKeyLengths: [1, 128],
    childValueLengths: [0, 256],
    children: {
      type: 'String',
    },
  },
  StreamName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
}

exports.custom = function(data) {
}

