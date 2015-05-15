#!/usr/bin/env node
var es = require('event-stream')

var argv = require('optimist')
  .usage('Usage: $0 --levels {MinZoom,MaxZoom} --attr {Attribute} --time {Time Attribute} -format {torque|time} --out {output-Directory} --pattern {features.*}')
  .demand(['attr','time'])
  .default('levels', '0,6')
  .default('out', './')
  .default('res', 'days')
  .default('append', false)
  .default('pattern', 'features.*')
  .default('format', 'torque')
  .argv

argv.out = __dirname + '/' + argv.out

var cublet = new require('../')(argv)

cublet.on('end', function(){
  console.log('done')
})

process.stdin
  .pipe(cublet.parser)
  .pipe(es.mapSync(function (data) {
    if (data){
      var coord = data.geometry.coordinates,
        value = data.properties[argv.attr],
        time = data.properties[argv.time]
        cublet.aggregate(coord, value, time)
    } else {
      console.log('Feature not aggregated, could not find the data-attribute or time-attribute', data)
    }
    return data
  }))

