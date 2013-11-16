Kinesalite (formerly kinesis-mock)
----------------------------------

[![Build Status](https://secure.travis-ci.org/mhart/kinesalite.png?branch=master)](http://travis-ci.org/mhart/kinesalite)

A mock implementation of [Amazon's Kinesis](http://docs.aws.amazon.com/kinesis/latest/APIReference/),
focussed on correctness and performance, and built on LevelDB
(well, [@rvagg](https://github.com/rvagg)'s awesome [LevelUP](https://github.com/rvagg/node-levelup) to be precise).

The Kinesis equivalent of [dynalite](https://github.com/mhart/dynalite).

To read and write from Kinesis streams in Node.js, consider using the [kinesis](https://github.com/mhart/kinesis)
module.

Example
-------

```sh
$ PORT=8000 kinesalite
```

Or programmatically:

```js
var kinesalite = require('kinesalite')

// Returns a standard Node.js HTTP server
var server = kinesalite()

// Listen on port 4567
server.listen(4567, function(err) {
  if (err) throw err
  console.log('Kinesalite started on port 4567')
})
```

Done
----

* CreateStream
* DeleteStream
* DescribeStream
* ListStreams

TODO
----

* GetNextRecords
* GetShardIterator
* PutRecord
* MergeShards
* SplitShard
