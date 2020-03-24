# MACA_climate_forLANDIS
script collection for processing climate files


Option 1 

thredd server url that shows all climate files for the nkn: 

http://thredds.northwestknowledge.net:8080/thredds/catalog.html

1. when you go to the above website, have to navigate to the correct dataset. 
2. in this case, i was interested in metdata relative humidity (min & max), navigated through to this: 
3. http://thredds.northwestknowledge.net:8080/thredds/reacch_climate_CMIP5_aggregated_macav2_catalog.html
4. but this is only a catalog of available, it does not allow you to download the netcdf from this page
5. click on each dataset *** this is where it could be improved!
6. get the url from (top of page) within the opendap link
7. this url then gets populated into python script to open that file



ORRRRRRRRRR, Option 2: 
 
maca portal where you can download netcdfs directly and with a created bounding box:

** well, sort of: it gives you a text file full of urls that should already be trimmed to the extent to bring into python

https://climate.northwestknowledge.net/MACA/data_portal.php


***from here, should load this text file into python! read in the list and would need to assign the file names, 
but this would be the quickest way to do it for future downloads? maybe?
