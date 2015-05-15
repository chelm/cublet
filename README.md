# cublet

## Usage 

* To use with GeoJSON: 

`cat data.geojson | node bin/geojson-cubes.js --levels 0,5 --attr Attribute --time Date-Attribute --out outdir`

* To use with a CSV file:
 
`cat storms.csv | node bin/csv-cubes.js --level 0,3 --lat 1 --lon 2 --attr 3 --time 0 -out tiles/storms`

Notice that with CSV we specify column numbers instead of names...


