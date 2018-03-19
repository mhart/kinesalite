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
  //TODO validate exactly one of [NextToken|StreamCreationTimestamp|StreamName]
  // NextToken: {
  //   type: 'String',
  //   regex: '[a-zA-Z0-9_.-]+',
  //   lengthGreaterThanOrEqual: 1,
  //   lengthLessThanOrEqual: 1048576,
  // },
  //TODO add Timestamp type
  // StreamCreationTimestamp: {
  //   type: 'Timestamp',
  //   notNull: true,
  // },
  StreamName: {
    type: 'String',
    notNull: true,
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 128,
  }
}
