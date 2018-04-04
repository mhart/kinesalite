exports.types = {
  ExclusiveStartShardId: {
    type: 'String',
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
  MaxResults: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 10000,
  },
  NextToken: {
    type: 'String',
    lengthGreaterThanOrEqual: 1,
  },
  StreamCreationTimestamp: {
    type: 'Timestamp',
  },
  StreamName: {
    type: 'String',
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  },
}
