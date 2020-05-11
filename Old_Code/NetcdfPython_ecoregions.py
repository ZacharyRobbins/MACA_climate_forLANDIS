# -*- coding: utf-8 -*-
"""
Created on Fri May 10 09:24:08 2019

@author: zjrobbin
"""
w_dir='E:/Maca_Climate_Files_Sapps/'

## Librarys
from datetime import datetime, timedelta
#from netCDF4 import num2date, date2num
#import matplotlib.pyplot as plt
import geopandas
import rasterio as rt 
import numpy as np
from netCDF4 import Dataset
from rasterio.mask import mask
from rasterio.crs import CRS
import pandas as pd
#from rasterio.plot import show
import os
import time

##Function
def getFeatures(gdf):
    """Function to parse features from GeoDataFrame in such a manner that rasterio wants them"""
    import json
    return [json.loads(gdf.to_json())['features'][0]['geometry']]


files=os.listdir(w_dir)

    
 
#listofruns=('RCP45Tempmin','RCP85Tempmin','RCP45Tempmax','RCPT85Tempmax','RCP45PPT','RCP85PPT')
listofruns=('RCP45Tempmin')
#files=('macav2livneh_tasmin_GFDL-ESM2M_r1i1p1_rcp45_2006_2099_CONUS_daily_aggregated',
#       'macav2livneh_tasmin_GFDL-ESM2M_r1i1p1_rcp85_2006_2099_CONUS_daily_aggregated',
#       'macav2livneh_tasmax_GFDL-ESM2M_r1i1p1_rcp45_2006_2099_CONUS_daily_aggregated',
#       'macav2livneh_tasmax_GFDL-ESM2M_r1i1p1_rcp85_2006_2099_CONUS_daily_aggregated',
#       'macav2livneh_pr_GFDL-ESM2M_r1i1p1_rcp45_2006_2099_CONUS_daily_aggregated',
#       'macav2livneh_pr_GFDL-ESM2M_r1i1p1_rcp85_2006_2099_CONUS_daily_aggregated')
files=('macav2livneh_tasmin_GFDL-ESM2M_r1i1p1_rcp45_2006_2099_CONUS_daily_aggregated')
key=('air_temperature','air_temperature','air_temperature','air_temperature','precipitation','precipitation')

key=('air_temperature')
#files=('macav2livneh_pr_GFDL-ESM2M_r1i1p1_rcp45_2006_2099_CONUS_daily_aggregated',
 #      'macav2livneh_pr_GFDL-ESM2M_r1i1p1_rcp85_2006_2099_CONUS_daily_aggregated')
#listofruns=('RCP45PPT','RCP85PPT')
#key=('precipitation','precipitation')
files=os.listdir(w_dir+"netCDFs/")
###Load in the Shapefile for the area in CRS: 4269 as climate outputs are. 
Shapeys=("High_Elevation_Dissolve","Low_elevation_Dissolved","Mid_El_Montane_Dissolve","North_Montane_Dissolved",)


for SH in Shapeys:
    AOI= geopandas.read_file((w_dir+'Climate_regions/'+SH+'.shp'))
    start=time.time()
    print(AOI)
    coords=getFeatures(AOI)
    ###Loop through climate files. 
    for r in list(range(0,(len(files)))):
        print(files[r])
        file=files[r]
        ####Get the keys based on the file names
        if "_pr_" in file:
            key='precipitation'
            model=file[16:]
            model=model.replace('_2006_2099_CONUS_daily_aggregated.nc',"")
        if "_tasmin_" in file:
            key='air_temperature'
            model=file[20:]
            model=model.replace('_2006_2099_CONUS_daily_aggregated.nc',"")      
        if "_tasmax_" in file:
            key='air_temperature' 
            model=file[20:]
            model=model.replace('i1p1_rcp85_2006_2099_CONUS_daily_aggregated.nc',"")
        if "_rcp85_" in file:
            scenario="RCP85"
        if "_rcp45_" in file:
            scenario="RCP45"
    
        #print((w_dir+'/netCDFs/'+files[r]+'.nc'))
        ### Load in the Net CDF file 
        Precip = Dataset((w_dir+'netCDFs/'+file), "r")
        
        #print(Precip.variables)
        #Precip['time']
        #for i in Precip.variables:
            #print(i)
        #print(Precip.variables['time'])
        #Get the array from the NETCDF
        Array= np.array(Precip.variables[key])
        ### Get Variables 
        Time=np.array(Precip.variables['time'])
        var=[key]
        #print(var)
        lat=np.array(Precip.variables['lat'])
        lon=np.array(Precip.variables['lon'])
        lon2=-(360-lon)
        
        ##Adjust dates
        #days since 1900-01-01
        ### Set standard dates
        dates = [datetime(1900,1,1)+n*timedelta(hours=24) for n in Time]
        
        ### Get meta data 
        out_meta={'crs':CRS.from_epsg(4269),
             'driver': 'GTiff',
             'count':34333,
             'dtype': 'float32',
             'height': len(lon2),
             'nodata': None,
             'transform':((max(lon2)-min(lon2))/len(lon2),0.0,min(lon2),0.0,-(max(lat)-min(lat))/len(lat),max(lat)), 
              #'transform': (min(lat), max(lat),(max(lat)-min(lat))/len(lat),min(lon),max(lon),(max(lon2)-min(lon2))/len(lon),max(lon)),
             'width': len(lat)}
        ###Write array as raster stack
        new_output=rt.open(w_dir+'All.tif', 'w', **out_meta) 
        new_output.write(Array)
        new_output.close()
        ### Get the Rasterstack
        Template=rt.open(w_dir+'All.tif')           
        print(Template)
        ### Create nulls   
        something=pd.DataFrame([[dates]],columns=["Timestep"])
        Meansmoosh=pd.DataFrame([[dates]],columns=["Timestep"])
        Varsmoosh=pd.DataFrame([[dates]],columns=["Timestep"])
        
       
            
        ###Mask
        out_img,out_transform=mask(Template,shapes=coords,crop=True,nodata=-9999)
        Template.bounds
        coords
        #More nulls
        MeanStack=pd.DataFrame(columns=["Timestep"])
        VarStack=pd.DataFrame(columns=["Timestep"])
        StdStack=pd.DataFrame(columns=["Timestep"])    
        ###Loop through dates to average     
        for i in list(range(1,len(dates))):
            Timestep=dates[i-200]
            #print(Timestep)
            band1=out_img[i,:,:]
            #print(band1)
             ### Fix temp K to C    
            meancalc=band1[band1!=-9999]
            if key == 'air_temperature':
                meancalc= meancalc-273.15
                #print(np.mean(meancalc))
           # print(meancalc)
            ### Get the Mean
            mean=(np.mean(meancalc))
            print(np.mean(mean))
            ### Variance 
            variance=(np.var(meancalc))
            ### Standard Deviation
            STD=(np.std(meancalc))
            ###Create Outputs
            Mean=pd.DataFrame([[Timestep,mean]],columns=["Timestep",key])
            StTime=pd.DataFrame([[Timestep,STD]],columns=['Timestep',key+"STD"])
            VarTime=pd.DataFrame([[Timestep,variance]],columns=['Timestep',(key+"VAR")])  
            ###Append to list    
            MeanStack=MeanStack.append(Mean)
            StdStack=StdStack.append(StTime)
            VarStack=VarStack.append(VarTime)
                
        #### Make into one dataframe        
        stepone=None    
            
        stepone=pd.merge(MeanStack,VarStack,how='inner', on='Timestep')
        one_eco=pd.merge(stepone,StdStack, how='inner',on='Timestep')
       
        one_eco.to_csv(w_dir+'Outputs/12_8/'+SH+'_'+model+scenario+key+'.csv')
        Template.close()
    end=time.time()
    print("Minutes elapsed "+str((end-start)/60))   
        
        
        
        

data=None
    ###endecoregion loop
    
daytomonth=daytomonth.append(oneday)
   

#os.remove(w_dir+'temp'+str(i)+'.tif')
    
    
Template.close()   
    
monthtoyear=monthtoyear.append(daytomonth)
monthtoyear.head
monthtoyear.to_csv(work_dir+"Outputs/"+str(year)+Model+".csv")
#





Template.profile
#show(Template,1)
###Template['Affine']
#Template.bounds

Template.close()
6697870.5-6656859.0
41011.5/1439
