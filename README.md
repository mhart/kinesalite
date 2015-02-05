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
$ kinesalite --help

Usage: kinesalite [--port <port>] [--path <path>] [options]

A mock Kinesis http server, optionally backed by LevelDB

Options:
--help                 Display this help message and exit
--port <port>          The port to listen on (default: 4567)
--path <path>          The path to use for the LevelDB store (in-memory by default)
--createStreamMs <ms>  Amount of time streams stay in CREATING state (default: 500)
--deleteStreamMs <ms>  Amount of time streams stay in DELETING state (default: 500)
--updateStreamMs <ms>  Amount of time streams stay in UPDATING state (default: 500)

Report bugs at github.com/mhart/kinesalite/issues
```

Or programmatically:

```js
// Returns a standard Node.js HTTP server
var kinesalite = require('kinesalite'),
    kinesaliteServer = kinesalite({path: './mydb', createStreamMs: 50})

// Listen on port 4567
kinesaliteServer.listen(4567, function(err) {
  if (err) throw err
  console.log('Kinesalite started on port 4567')
})
```

Installation
------------

With [npm](http://npmjs.org/) do:

```sh
$ npm install -g kinesalite
```

Done
----

* All validations
* CreateStream
* DeleteStream
* DescribeStream
* ListStreams
* PutRecord
* PutRecords
* GetShardIterator

TODO
----

* GetRecords
* MergeShards
* SplitShard
