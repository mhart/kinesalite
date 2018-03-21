exports.types = {
   StreamName: {
     type: 'String',
     notNull: true,
     regex: '[a-zA-Z0-9_.-]+',
     lengthGreaterThanOrEqual: 1,
     lengthLessThanOrEqual: 128,
   },
   Limit: {
     type: 'Integer',
     greaterThanOrEqual: 1,
     lessThanOrEqual: 10000,
   },
   ExclusiveStartShardId: {
     type: 'String',
     regex: '[a-zA-Z0-9_.-]+',
     lengthGreaterThanOrEqual: 1,
     lengthLessThanOrEqual: 128,
   },
 }