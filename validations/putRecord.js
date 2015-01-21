exports.types = {
  SequenceNumberForOrdering: {
    type: 'String',
    regex: '0|([1-9]\\d{0,128})',
  },
  PartitionKey: {
    type: 'String',
    notNull: true,
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 256,
  },
  ExplicitHashKey: {
    type: 'String',
    regex: '0|([1-9]\\d{0,38})',
  },
  Data: {
    type: 'Blob',
    notNull: true,
    lengthLessThanOrEqual: 51200,
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

