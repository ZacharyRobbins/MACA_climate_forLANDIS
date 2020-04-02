

## Librarys
import xarray as xr
import os
import time
import numpy as np

### It says you have to import netcdf4 before rasterio or else an error will occur
from netCDF4 import Dataset
from datetime import datetime, timedelta
#from netCDF4 import num2date, date2num
#import matplotlib.pyplot as plt
import geopandas as gp
import rasterio as rt 
from rasterio.transform import from_origin
from rasterio.mask import mask
from rasterio.crs import CRS
import pandas as pd
import dask
from functools import partial
from joblib import Parallel, delayed ,cpu_count
#import matplotlib.pyplot as plt


####  This is my work on top of K. Jones xrray grab for netcdfs from the maca threads
####  Authors K. Jones, Z. Robbins 2020 ***Pandemic Code***



### Below I am reading in individual URL's from NKN Thredd Server, however, this could be modified to take in the text file produced 
### by the MACA portal download and 
### read the url list within the text file. Since the URLs can be generated to download netcdfs at a smaller bounding 
### box extent (from the maca website), it would eliminate the need to do the
### "slicing" operation in the loop at the bottom of this script.

## Functions


def getFeatures(gdf):
    """Function to parse features from GeoDataFrame in such a manner that rasterio wants them"""
    import json
    return [json.loads(gdf.to_json())['features'][0]['geometry']]

def Loadinandcut(httpline,latmin,latmax,lonmin,lonmax):
   ##Function to take a MACA data library line, change the server,cut it to the"""
   ##Appropriate size for your study extent and turn into netcdf4"""
    ###Setup outputs
    #httpline=lines[5]
    start=time.time()
    trimmed_name = httpline.split('macav2metdata_')[1]
    modelname = trimmed_name.split('_daily')[0]
    ###GetNetcddf
    removespatial=httpline.split("?")[0]
        ### Here we are readjusting the server
    Gethalf=removespatial.split("ncss/grid/")[0]
    GetOtherHalf=removespatial.split("ncss/grid/")[1]
        ### Get a new filename to search
    file=Gethalf+"dodsC/"+GetOtherHalf
    ###Xarray File
    print('Load',(time.time() - start))
    
    Testfile = xr.open_dataset(file)
#    with xr.open_dataset(file) as ds:
 #       ds.load()

    ### Cut to the area 
    print('Slice',(time.time() - start))
    file_slice = Testfile.sel(lon=slice(lonmin, lonmax), lat=slice(latmin, latmax))
    ###Print and cover your tracks
    print('Write',(time.time() - start))    
    #file_slice.chunk(chunks={'time':500})
    #start=time.time()
    file_slice.to_netcdf(Drive+modelname+".nc",engine="scipy")
    print('Done',(time.time() - start))  
    ##Time is 6:11
    print("1")
    file_slice.close()
    print("2")
    Testfile.close()
    print("3")

    
    
def FormatNetCdf(input_cdf,key):
    ##Function to take Netcdf and format for rasterio as a multiband tiff"""
    #start=time.time()
    #print("start")
    ##Load in netcdf4
    Precip = Dataset(input_cdf,"r")
    
    #Get the array from the NETCDF
    Array= np.array(Precip.variables[key])
    ## Not sure why I have to flip, but it seems to be writing in reverse order
    ## This fix works 
    Array=np.flip(Array,axis=1)
    ### Get Variables 
    Time=np.array(Precip.variables['time'])
    dates = [datetime(1900,1,1)+n*timedelta(hours=24) for n in Time]
    ## Get geo for locating 
    lat=np.array(Precip.variables['lat'])
    lon=np.array(Precip.variables['lon'])
    lon2=-(360-lon)
    
    ##Adjust dates
    #days since 1900-01-01
    ### Set standard dates
    ### Set resolutions
    reslon=(max(lon2)-min(lon2))/len(lon2)
    reslat=(max(lat)-min(lat))/len(lat)
    ### Transform structure in form West North, Widthres  Heighres. 
    transform_Structure=from_origin(min(lon2), max(lat), reslon, reslat)
    
    ### Get meta data 
    out_meta={'crs':CRS.from_epsg(4269),
    'driver': 'GTiff',
    'count':34333,
    'dtype': 'float32',
    'height': len(lat),
    'nodata': None,
    'transform':transform_Structure, 
    #'transform': (min(lat), max(lat),(max(lat)-min(lat))/len(lat),min(lon),max(lon),(max(lon2)-min(lon2))/len(lon),max(lon)),
    'width': len(lon2)}
       ###Write array as raster stack
      
    new_output=rt.open(Drive+'All.tif', 'w', **out_meta) 
    new_output.write(Array)
    new_output.close()
    Template=rt.open(Drive+'All.tif')
    return(Template,dates)

def MaskandWrite(shapefile,key,Template,dates,label):
    ##Function to mask the Tiff by a shapefile, and return a set of ecoregion variable data"""
    start=time.time()
    print("start")
    ###Ensure shapefile is dissolved
    geoshape=shapefile.dissolve(by='DN')
    coords=getFeatures(geoshape)   
    ### Get the Rasterstack
               

    ### Create nulls   
    #something=pd.DataFrame([[dates]],columns=["Timestep"])
    MeanStack=pd.DataFrame(columns=["Timestep"])
    VarStack=pd.DataFrame(columns=["Timestep"])
    StdStack=pd.DataFrame(columns=["Timestep"])    
                    
    ###Mask
    out_img,out_transform=mask(Template,shapes=coords,crop=True,nodata=-9999)
    print('MaskDone',(time.time() - start))
    for i in list(range(1,len(dates))):
        
        Timestep=dates[i]
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
        #print(np.mean(mean))
        ### Variance 
        variance=(np.var(meancalc))
        ### Standard Deviation
        STD=(np.std(meancalc))
        ###Create Outputs
        Mean=pd.DataFrame([[Timestep,mean]],columns=["Timestep",label])
        StTime=pd.DataFrame([[Timestep,STD]],columns=['Timestep',label+"STD"])
        VarTime=pd.DataFrame([[Timestep,variance]],columns=['Timestep',(label+"VAR")])  
        ###Append to list    
        MeanStack=MeanStack.append(Mean)
        StdStack=StdStack.append(StTime)
        VarStack=VarStack.append(VarTime)
    print('Writingdone',(time.time() - start))    
    #### Make into one dataframe        
    stepone=None    
            
    stepone=pd.merge(MeanStack,VarStack,how='inner', on='Timestep')
    one_eco=pd.merge(stepone,StdStack, how='inner',on='Timestep')
    print('Done',(time.time() - start))  
    return(one_eco)

##### Running the stuff 


### Set drive Location
Drive="C:/Users/zjrobbin/Desktop/New_Climate/"
#### Load in Shapes
Shape1= gp.read_file((Drive+'New_Climate_Regions/done/Eco_1.shp'))
Shape2= gp.read_file((Drive+'New_Climate_Regions/done/Eco_2.shp'))
Shape3= gp.read_file((Drive+'New_Climate_Regions/done/Eco_3.shp'))
Shape4= gp.read_file((Drive+'New_Climate_Regions/done/Eco_4.shp'))
Shape5= gp.read_file((Drive+'New_Climate_Regions/done/Eco_5.shp'))
Shape6= gp.read_file((Drive+'New_Climate_Regions/done/Eco_6.shp'))
Shape8= gp.read_file((Drive+'New_Climate_Regions/done/Eco_8.shp'))
Shape9= gp.read_file((Drive+'New_Climate_Regions/done/Eco_9.shp'))
Shape10= gp.read_file((Drive+'New_Climate_Regions/done/Eco_10.shp'))
Shape11= gp.read_file((Drive+'New_Climate_Regions/done/Eco_11.shp'))


Full_start=time.time()
### This is the Text File
filepath =Drive+ 'macav2_OneModel_Test.txt'

with open(filepath) as fp:
    Lineread = fp.readlines()### This is the individual files we need to dl
    
### the last line is always blank, remove it. 
del Lineread[-1]

### Here I am removing the Last Lines    


#Inputs
#Loadinandcut2(lines[0], latmin, latmax, lonmin, lonmax)
### Set a reasonable bounding box for the first cut. 
lonmin=274.4312 
lonmax=279.8369 
latmin=33.9681
latmax=37.0724


### Create a partial (ei a model with all but one varaible for parallel processing)
partLoadandCut=partial(Loadinandcut,latmin=latmin,latmax=latmax,lonmin=lonmin,lonmax=lonmax)

Models=1


for j in list(range(0,Models)):
    Full_start=time.time()
    start=0+(8*j)
    end=start+8
    ### I put this in to manage memeory 
    ### (Only process one models worth of varibles at a time)

    
    lines=Lineread[start:end]
    httplut=[lines[0],lines[1],lines[2],lines[3],lines[4],lines[5],lines[6],lines[7]]
    
    
    ### I have had trouble getting the par to reliably not get locked out or 
    ### post a HDF error. So for now we are singular.
    #Parallel(n_jobs=6)(delayed(partLoadandCut)(httpline=i) for i in httplut)

    ### I set up this version to to load in of all netCDF's then process each 
    ### of them. This was where I was having the most problems. 
    for i in httplut:
        start1=time.time()
        print("Loading in Data",time.time()-start1,)
        partLoadandCut(httpline=i)
        
        #httpline=i
        print("DataLoaded",time.time()-start1)
    
    
    
    
    for i in httplut:
        print(i)
        
        start1=time.time()
        httpline=i
        print("Preparing Variables",time.time()-start1)
        ### Set up lookups 
        trimmed_name = httpline.split('macav2metdata_')[1]
        #print(trimmed_name)
        modelname = trimmed_name.split('_daily')[0]
        #print(modelname)
        variablename=modelname.split('_')[0]
        #print(variablename)
        ### 
        NC_path=(Drive+modelname+".nc")
        par=variablename
        #print(par)
        if "pr" == par:
            key='precipitation'
            #model=file[16:]
            #model=model.replace('_2006_2099_CONUS_daily_aggregated.nc',"")
        if "tasmin"== par:
            key='air_temperature'
            #model=file[20:]
            #model=model.replace('_2006_2099_CONUS_daily_aggregated.nc',"")      
        if "tasmax" == par:
            key='air_temperature' 
            #model=file[20:]
            #model=model.replace('i1p1_rcp85_2006_2099_CONUS_daily_aggregated.nc',"")
        if"rhsmax"== par:
            key='relative_humidity'
        if"rhsmin"== par:
            key='relative_humidity' 
        if"vpd"== par:    
            key='vpd'
        if "uas"== par:
            key="eastward_wind"
        if "vas"== par:
            key="northward_wind"
        
        
        Template,dates=FormatNetCdf(NC_path, key)
        
        ### masking and writing matrices
        #MaskandWritepart=partial(MaskandWrite, key, Template, dates,"eco2")
        #start=time.time()
        #shapes=[Shape1,Shape2,Shape3,Shape4]
        #Ecos=zip(*Parallel(n_jobs=4)(delayed(MaskandWritepart)(shapefile=i) for i in shapes))
        #elapsed = (time.time() - start)    
        print("Cutting_Begins",time.time() - start1) 
        practiceoneeco1=MaskandWrite(Shape1,key,Template,dates,"eco2")
        practiceoneeco2=MaskandWrite(Shape2,key,Template,dates,"eco3")
        practiceoneeco3=MaskandWrite(Shape3,key,Template,dates,"eco4")
        practiceoneeco4=MaskandWrite(Shape4,key,Template,dates,"eco5")
        practiceoneeco5=MaskandWrite(Shape5,key,Template,dates,"eco6")
        practiceoneeco6=MaskandWrite(Shape6,key,Template,dates,"eco7")
        practiceoneeco7=MaskandWrite(Shape8,key,Template,dates,"eco8")
        practiceoneeco8=MaskandWrite(Shape9,key,Template,dates,"eco9")
        practiceoneeco9=MaskandWrite(Shape10,key,Template,dates,"eco10")
        practiceoneeco10=MaskandWrite(Shape11,key,Template,dates,"eco11")
        elapsed = (time.time() - start1)
        print("Cutting done",elapsed)
        
        ###ECOLUT
        ##High Elevation is eco 5
        ##NorthMontane is 2
        ## Mid Montane is 3
        ## Low Elevation is 4
        
        
        
        ### Merging one dataframe this could be cleaned 
        stepone=pd.merge(practiceoneeco1,practiceoneeco2,how='inner', on='Timestep')
        steptwo=pd.merge(stepone,practiceoneeco3,how='inner', on='Timestep')
        stepthree=pd.merge(steptwo,practiceoneeco4,how='inner', on='Timestep')
        step4=pd.merge(stepthree,practiceoneeco5,how='inner', on='Timestep')
        step5=pd.merge(step4,practiceoneeco6,how='inner', on='Timestep')
        step6=pd.merge(step5,practiceoneeco7,how='inner', on='Timestep')
        step7=pd.merge(step6,practiceoneeco8,how='inner', on='Timestep')
        step8=pd.merge(step7,practiceoneeco9,how='inner', on='Timestep')
        step9=pd.merge(step8,practiceoneeco10,how='inner', on='Timestep')
        step9.columns.tolist()
        
        ### Order Columns 
        colorder=["Timestep","eco2","eco3","eco4","eco5","eco6","eco7","eco8","eco9",
                  "eco10","eco11","eco2VAR","eco3VAR","eco4VAR","eco5VAR","eco6VAR","eco7VAR",
                  "eco8VAR","eco9VAR","eco10VAR","eco11VAR",
                  "eco2STD","eco3STD","eco4STD","eco5STD","eco6STD","eco7STD","eco8STD","eco9STD",
                  "eco10STD","eco11STD"]
        ### Make it pretty 
        Print_Ready=step9[colorder]
        
        
        Print_Ready.to_csv(Drive+"Outputs/"+modelname+".csv")
        os. remove(Drive+modelname+".nc") 
    
    print("Model finished")
    print(time.time()-Full_start)
