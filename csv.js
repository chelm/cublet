var csv = require("csv"),
  tilePixel = require("tile-pixel"),
  es = require('event-stream'),
  fs = require('fs'),
  async = require('async'),
  mkdirp = require('mkdirp')

var argv = require('optimist')
  .usage('Usage: $0 -levels MinZoom,MaxZoom -lat colNum -lon colNum -attr colNum -time colNum -style {torque|time}')
  .default('level', '0,6')
  .default('attr', null)
  .default('time', null)
  .default('lat', null)
  .default('lon', null)
  .default('style', 'torque')
  .argv

var levels = argv.level.split(',')

var agg = {},
  maxTime, minTime

var q = async.queue(function (t, cb) {
  mkdirp(t.path, function (err) {
    //var pxData = {}
    var pxData = []

    for (var px in t.tile) {
      var xy = px.split('-')
      var times = []
      var values = []

      for (var time in t.tile[px].values){
        var epoch = new Date(parseInt(time)).getTime()
        //console.log(epoch, time, minTime)
        var day = ~~((epoch-minTime)/(1000*60*60*24))
        // Torque style tiles 
        times.push(day)
        values.push(t.tile[px].values[time])

        // Time based tiles
        if (argv.s === 'time'){ 
          if (!pxData[day]){
            pxData[day] = []
          }
          pxData[day].push({
            x: xy[0],
            y: xy[1],
            v: t.tile[px].values[time]
          })
        }
      }
      if (argv.style === 'torque'){
        pxData.push({
          x: xy[0],
          y: xy[1],
          t: times,
          v: values
        })
      }
    }
    fs.writeFileSync(t.path+'/'+t.file, JSON.stringify(pxData))
    cb()
  })
},4)

q.drain = function(){
  console.log('Min Time:', minTime)
  console.log('Max Time:', maxTime);
  console.log(~~((maxTime-minTime)/(1000*60*60*24)), 'Total days')
}

var stream = process.stdin
  .pipe(csv.parse())
  .pipe(csv.transform(function(data){
    var coords = [parseFloat(data[argv.lon]), parseFloat(data[argv.lat])],
      value = data[argv.attr],
      time = data[argv.time]
    // --------------
    // parse time hack for the storm data only
    var year = time.substr(0,4),
      month = time.substr(4,2),
      day = time.substr(6,2);
    var date = new Date(year, parseInt(month)-1, parseInt(day)-1).getTime()
    // --------------

    minTime = (!minTime) ? date : Math.min(date, minTime)
    maxTime = (!maxTime) ? date : Math.max(date, maxTime)

    tilePixel(coords[0], coords[1], levels[0], parseInt(levels[1])+1, function (err, pxAgg) {
      for (var z in pxAgg){
        if (!agg[z]){
          agg[z] = {}
        }
        var tile = pxAgg[z].tile.join('_')
        if (!agg[z][tile]){
          agg[z][tile] = {}
        }
        var index = pxAgg[z].px +'-'+ pxAgg[z].py
        if (!agg[z][tile][index]){
          agg[z][tile][index] = { count: 0, values: {}}
        }
        agg[z][tile][index].values[date] = value
        agg[z][tile][index].count++
      }
    })
    
  }))

stream.on('finish', function end(){
  // create tiles 
  console.log('end');
  for (var z in agg){
    for (var tile in agg[z]){
      var xyz = tile.split('_')
      var path = [__dirname, argv.o, xyz[2], xyz[0]].join('/')
      q.push({path: path, file: xyz[1]+'.json', tile: agg[z][tile]}, function(){})
    }
  }
});

