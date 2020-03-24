
import xarray as xr
import netCDF4
import os
import matplotlib.pyplot as plt

### Below I am reading in individual URL's copy and pasted from the text file, however, this could be modified to take in a text file and read the url list within
### the text file. Since the URLs can be generated to download netcdfs at a smaller bounding box extent (from the maca website), it would eliminate the need to do the
### "slicing" operation in the loop at the bottom.

#read in urls for each climate projection min/max
inmcm4_rcp85_rhsmin_url = 'http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_macav2metdata_rhsmin_inmcm4_r1i1p1_rcp85_2006_2099_CONUS_daily.nc'
inmcm4_rcp85_rhsmax_url = 'http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_macav2metdata_rhsmax_inmcm4_r1i1p1_rcp85_2006_2099_CONUS_daily.nc'
canesm2_rcp85_rhsmin_url = 'http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_macav2metdata_rhsmin_CanESM2_r1i1p1_rcp85_2006_2099_CONUS_daily.nc'
canesm2_rcp85_rhsmax_url = 'http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_macav2metdata_rhsmax_CanESM2_r1i1p1_rcp85_2006_2099_CONUS_daily.nc'
miroc_rcp85_rhsmin_url = 'http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_macav2metdata_rhsmin_MIROC-ESM-CHEM_r1i1p1_rcp85_2006_2099_CONUS_daily.nc'
miroc_rcp85_rhsmax_url = 'http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_macav2metdata_rhsmax_MIROC-ESM-CHEM_r1i1p1_rcp85_2006_2099_CONUS_daily.nc'

#make list of urls
urls = [inmcm4_rcp85_rhsmin_url,inmcm4_rcp85_rhsmax_url,canesm2_rcp85_rhsmax_url,canesm2_rcp85_rhsmin_url, miroc_rcp85_rhsmax_url, miroc_rcp85_rhsmin_url]

#trim the urls strings to be the model names without other url hodgepodge
trimmed = [i.split('agg_')[1] for i in urls]


### could NOT find ccsm for METDATA relative humidity
#ccsm_rcp85_rhsmin_url =
#ccsm_rcp85_rhsmax_url =

# open min/max urls for each climate projection, pulling them virtually, not locally
inmcm4_rhsmin = xr.open_dataset(inmcm4_rcp85_rhsmin_url)
inmcm4_rhsmax = xr.open_dataset(inmcm4_rcp85_rhsmax_url)
canesm2_rhsmin = xr.open_dataset(canesm2_rcp85_rhsmin_url)
canesm2_rhsmax = xr.open_dataset(canesm2_rcp85_rhsmax_url)
miroc_rhsmin = xr.open_dataset(miroc_rcp85_rhsmin_url)
miroc_rhsmax = xr.open_dataset(miroc_rcp85_rhsmax_url)
#ccsm_rhsmin = xr.open_dataset(ccsm_rcp85_rhsmin_url)
#ccsm_rhsmax = xr.open_dataset(ccsm_rcp85_rhsmax_url)

#make a list of those opened datasets
file_list = [inmcm4_rhsmin, inmcm4_rhsmax, canesm2_rhsmin, canesm2_rhsmax, miroc_rhsmin, miroc_rhsmax] #, ccsm_rhsmin, ccsm_rhsmax]


### BELOW SCRATCH WORK FOR plotting

#test plotting and opening to make sure things are what I think they are
# test = inmcm4_rhsmax.isel(time=0)
# rh = test['relative_humidity']
# lat = test['lat']
# lon = test['lon']

# THIS BIT IS OUT OF ORDER , THIS IS ME PLOTTING A SINGLE SLICE FROM THE SLICES CREATED BELOW #####
# #more testing after the slice
# rh_sl = inmcm4_rhmin_sl['relative_humidity']
# lat_sl = inmcm4_rhmin_sl['lat']
# lon_sl = inmcm4_rhmin_sl['lon']
# plt.pcolor(lon_sl, lat_sl, rh_sl)

# plt.pcolor(lon, lat, rh)
#### END SCRATCH WORK FOR PLOTTING


#  now, want to trim netcdfs by bounding box before saving them locally
#  set up a loop to trim and write each one out

#  path to write out netcdfs to write out netcdfs
path = "R:/fer/rschell/Jones/MACA_climate_scenarios/trimmed_netcdfs/"


for i in range(len(file_list)):

    file_name = trimmed[i]

    #  designate the lat long to trim by
    file_slice = file_list[i].sel(lon=slice(274.4312, 279.8369), lat=slice(33.9681, 37.0724))

    full_path = os.path.join(path, file_name)

    file_slice.to_netcdf(full_path)