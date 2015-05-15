var tilePixel = require("tile-pixel"),
  fs = require('fs'),
  async = require('async'),
  events = require('events'),
  mkdirp = require('mkdirp')

/**
 * Init Cublet with options: 
 *  - pattern: 'features.*',
 *  - levels: [0, 5]
 *  - outDir: 'out-tile-dir'
 */
module.exports = function (options) {
  var cublet = new events.EventEmitter()
  var levels = options.levels.split(',') || [0, 5]
  var agg = {},
    maxTime, minTime

  if (options.append){
    var specFile = options.out+'/spec.json'
    fs.exists(specFile, function (exists) {
      if (exists) {
        var spec = JSON.parse(fs.readFileSync(specFile).toString());
        maxTime = spec.maxTime
        minTime = spec.minTime
      }
    })
  }

  var resolutions = {
    days: (1000*60*60*24),
    hours: (1000*60*60),
    minutes: (1000*60),
    seconds: 1000
  }

  // create the parser
  // could be dynamic to support csv vs json parsing
  if (options.type && options.type === 'csv'){
    cublet.parser = require("csv").parse()
    cublet.parser.on('finish', function () {
      cublet.writeTiles(agg)
    })
  } else {
    cublet.parser = require("JSONStream").parse(options.pattern || 'features.*')
    cublet.parser.on('end', function () {
      cublet.writeTiles(agg)
    })
  }

  cublet.writeTiles = function (agg) {

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
            var date = ~~((epoch-minTime)/(resolutions[options.res]))
            // Torque style tiles 
            times.push(date)
            values.push(t.tile[px].values[time])

            // Time based tiles
            if (options.format === 'time'){ 
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
          if (options.format === 'torque'){
            pxData.push({
              x: xy[0],
              y: xy[1],
              t: times,
              v: values
            })
          }
        }
        var filePath = t.path+'/'+t.file
        if (options.append) {
          fs.exists(filePath, function (exists) {
            if (exists){
              // open the existing file
              var fileData = JSON.parse(fs.readFileSync(filePath).toString())
              fileData.forEach(function (fRow, i) {
                pxData.forEach(function (pRow) {
                  if (fRow.x === pRow.x && fRow.y === pRow.y) {
                    fileData[i].t = fRow.t.concat(pRow.t)
                    fileData[i].v = fRow.v.concat(pRow.v)
                  }
                })
              })
              // append data where x/y match
              fs.writeFileSync(filePath, JSON.stringify(fileData))
            } else {
              fs.writeFileSync(filePath, JSON.stringify(pxData))
            }
          })
        } else {
          console.log(pxData)
          fs.writeFileSync(filePath, JSON.stringify(pxData))
        }
        cb()
      })
    },4)

    q.drain = function(){
      // TODO Save the tile.json spec file w:
      // min/max time, 
      // bbox, 
      // temporal resolution (seconds, minutes, hours, days)
      var spec = {
        res: options.res,
        minTime: minTime,
        maxTime: maxTime
      }
      fs.writeFileSync(options.out+'/spec.json', JSON.stringify(spec));
      console.log('Min Time:', minTime)
      console.log('Max Time:', maxTime);
      console.log(~~((maxTime-minTime)/resolutions[options.res]), 'Total '+ options.res);
      cublet.emit('end')
    }

    // create tiles
    for (var z in agg){
      for (var tile in agg[z]){
        var xyz = tile.split('_')
        var path = [options.out, xyz[2], xyz[0]].join('/')
        q.push({path: path, file: xyz[1]+'.json', tile: agg[z][tile]}, function(){})
      }
    }
  }

  cublet.aggregate = function (coords, value, time) {
    var date = new Date(time)    
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
        agg[z][tile][index].values[time] = value
        agg[z][tile][index].count++
      }
    })
    return
  }

  return cublet
}

