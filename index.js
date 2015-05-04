var stream = require("JSONStream"),
  tilePixel = require("./aggregation/"),
  es = require('event-stream'),
  fs = require('fs'),
  async = require('async'),
  mkdirp = require('mkdirp')

var argv = require('optimist')
  .usage('Usage: $0 -l MinZoom,MaxZoom -a Attribute -t TimeAttribute')
  .default('l', '0,6')
  .default('a', null)
  .default('t', null)
  .argv

var levels = argv.l.split(',');

var agg = {},
  maxTime, minTime

var q = async.queue(function (t, cb) {
  mkdirp(t.path, function (err) {
    var pxData = []
    for (var px in t.tile) {
      var xy = px.split('-')
      var times = [];
      var values = [];
      for (var time in t.tile[px].values){
        var date = new Date(time)
        if (date.getTime() < minTime || !minTime){
          minTime = date;
        }
        if (date.getTime() > maxTime || !maxTime){
          maxTime = date;
        }
        times.push(date)
        values.push(t.tile[px].values[time])
      }
      pxData.push({
        x: xy[0],
        y: xy[1],
        t: times,
        v: values 
      })
    }
    fs.writeFileSync(t.path+'/'+t.file, JSON.stringify(pxData))
    cb()
  })
},4)

q.drain = function(){
  console.log('DONE', minTime, maxTime);
}

var parser = stream.parse('features.*');
parser.on('end', function () {

  // create tiles 
  for (var z in agg){
    for (var tile in agg[z]){
      var xyz = tile.split('_')
      var path = [__dirname, argv.o, xyz[2], xyz[0]].join('/')
      q.push({path: path, file: xyz[1]+'.json', tile: agg[z][tile]}, function(){})
    }
  }
});

process.stdin
  .pipe(parser)
  .pipe(es.mapSync(function (data) {
    var coords = data.geometry.coordinates,
      value = data.properties[argv.a],
      time = data.properties[argv.t]

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
        agg[z][tile][index].values[time] = value
        agg[z][tile][index].count++
      }
    })
    return data
  }));

