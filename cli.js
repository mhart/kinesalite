#!/usr/bin/env node

var argv = require('minimist')(process.argv.slice(2))

if (argv.help) {
  return console.log([
    '',
    'Usage: kinesalite [--port <port>] [--path <path>] [options]',
    '',
    'A mock Kinesis http server, optionally backed by LevelDB',
    '',
    'Options:',
    '--help                 Display this help message and exit',
    '--port <port>          The port to listen on (default: 4567)',
    '--path <path>          The path to use for the LevelDB store (in-memory by default)',
    '--createStreamMs <ms>  Amount of time streams stay in CREATING state (default: 500)',
    '--deleteStreamMs <ms>  Amount of time streams stay in DELETING state (default: 500)',
    '--updateStreamMs <ms>  Amount of time streams stay in UPDATING state (default: 500)',
    '',
    'Report bugs at github.com/mhart/kinesalite/issues',
  ].join('\n'))
}

require('./index.js')(argv).listen(argv.port || 4567)
