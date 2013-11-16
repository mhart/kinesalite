exports.types = {
  Data: {
    type: 'String',
    required: true,
    lengthLessThanOrEqual: 51200,
  },
  ExclusiveMinimumSequenceNumber: {
    type: 'String',
    lengthGreaterThanOrEqual: 1,
  },
  ExplicitHashKey: {
    type: 'String',
    lengthGreaterThanOrEqual: 1,
  },
  PartitionKey: {
    type: 'String',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 256,
  },
  StreamName: {
    type: 'String',
    required: true,
    streamName: true,
    regex: '[a-zA-Z0-9_.-]+',
  },
}

exports.custom = function(data) {
}

