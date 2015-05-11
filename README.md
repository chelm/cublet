# cublet

## Usage 

* To use with GeoJSON: 

`cat data.geojson | node index.js -l 0,5 -a Attribute -t Date-Attribute -o outdir`


* To use with a CSV file:
 
`cat storms.csv | node csv.js --level 0,3 --lat 1 --lon 2 --attr 3 --time 0 -o tiles/storms`

Notice that with CSV we specify column numbers instead of names...
