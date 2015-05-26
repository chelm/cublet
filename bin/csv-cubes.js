#!/usr/bin/env node
var csv = require('csv')

var argv = require('optimist')
  .usage('Usage: $0 -levels MinZoom,MaxZoom -lat colNum -lon colNum -attr colNum -time colNum -style {torque|time}')
  .demand('attr', 'time', 'lat', 'lon')
  .default('levels', '0,6')
  .default('res', 'days')
  .default('append', false)
  .default('out', './')
  .default('format', 'torque')
  .argv

console.log(argv)

argv.type = 'csv'
argv.out = __dirname + '/' + argv.out

var cublet = new require('../')(argv)

cublet.on('end', function(){
  console.log('done')
})

process.stdin
  .pipe(cublet.parser)
  .pipe(csv.transform(function(data){
    var coords = [parseFloat(data[argv.lon]), parseFloat(data[argv.lat])],
      value = data[argv.attr],
      //time = new Date(data[argv.time]).getTime()
      time = data[argv.time]

    // --------------
    // parse time hack for the storm data only
    var year = time.substr(0,4),
      month = time.substr(4,2),
      day = time.substr(6,2)

    time = new Date(year, parseInt(month)-1, parseInt(day)-1).getTime()
    // --------------

    cublet.aggregate(coords, value, time)
  }))

