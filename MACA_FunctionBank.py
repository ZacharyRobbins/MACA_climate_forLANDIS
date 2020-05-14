# -*- coding: utf-8 -*-
"""
Created on Mon May 11 12:08:26 2020

@author: zacha
"""

####  This is my work on top of K. Jones xrray grab for netcdfs from the maca threads
####  Authors K. Jones, Z. Robbins 2020 ***Pandemic Code***



### Below I am reading in individual URL's from NKN Thredd Server, however, this could be modified to take in the text file produced 
### by the MACA portal download and 
### read the url list within the text file. Since the URLs can be generated to download netcdfs at a smaller bounding 
### box extent (from the maca website), it would eliminate the need to do the
### "slicing" operation in the loop at the bottom of this script.
### This set can only handle variables for Tmin,Tmax,RhMax,RhMin,WindSpeed and
### Direction. 






###MACA Processing Function Bank
import xarray as xr
import os
import sys
import time
import numpy as np
### It says you have to import netcdf4 before rasterio or else an error will occur
from netCDF4 import Dataset
from datetime import datetime, timedelta
import fnmatch
import geopandas as gp
import rasterio as rt 
from rasterio.transform import from_origin
from rasterio.mask import mask
from rasterio.crs import CRS
import pandas as pd
from functools import partial
from joblib import Parallel, delayed ,cpu_count

def ModelParser(Drive,Modelname,filepath,EcoregionBeginswith,EcoregionEndswith,path,shapefiles):
   Ecolen=list(range(EcoregionBeginswith,EcoregionEndswith+1))
   Ecos=[]
   for I in Ecolen:
        Ecos.append("eco"+str(I))
        #### Load in Shapes
   print("Checking for shape files: ")
   for I in shapefiles:
       print("     "+I)
       filesareat=(Drive+path)
       try:
           Shape1= gp.read_file(filesareat+I+'.shp')
           #break
       except ValueError:
           print("Missing a Ecoregion "+ I+ " File/File named incorrectly")
   with open(filepath) as fp:
        Lineread = fp.readlines()
   del Lineread[-1]
   Modelfiles_MP=[y for y in Lineread if Modelname in y]
   Modelfiles_MP=[y for y in Modelfiles_MP if "vpd" not in y]
   print("Running " + Modelname+ " With "+str(len(Modelfiles_MP))+" Files ")
   print("With data for")
   for i in Modelfiles_MP:
        trimmed_name = i.split('macav2metdata_')[1]
        Modelname = trimmed_name.split('_daily')[0]
        print("       "+Modelname .split('_')[0])
   return(Ecos,Modelfiles_MP)


def getFeatures(gdf):
    """Function to parse features from GeoDataFrame in such a manner that rasterio wants them"""
    import json
    return [json.loads(gdf.to_json())['features'][0]['geometry']]

def Loadinandcut(Drive,httpline,latmin,latmax,lonmin,lonmax):
   ##Function to take a MACA data library line, change the server,cut it to the"""
   ##Appropriate size for your study extent and turn into netcdf4"""
    ###Setup outputs
    start=time.time()
    trimmed_name = httpline.split('macav2metdata_')[1]
    Modelname = trimmed_name.split('_daily')[0]
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
    ### Cut to the area 
    print('Slice',(time.time() - start))
    file_slice = Testfile.sel(lon=slice(lonmin, lonmax), lat=slice(latmin, latmax))
    ###Print and cover your tracks
    print('Write',(time.time() - start))    
    #file_slice.chunk(chunks={'time':500})
    #start=time.time()
    file_slice.to_netcdf(Drive+"/Temp/"+Modelname+".nc",engine="scipy")
    print('Done',(time.time() - start))  
    file_slice.close()
    Testfile.close()


    
    
def FormatNetCdf(Drive,input_cdf,key):
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
      
    new_output=rt.open(Drive+'Temp/All.tif', 'w', **out_meta) 
    new_output.write(Array)
    new_output.close()
    Template=rt.open(Drive+'Temp/All.tif')
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
    for i in list(range(0,len(dates))):
        
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

### Maca presents wind as uas/vas and LANDIS-II wants wind in windspeed and 
### directions, this function does that and then cleans out the other file. 


def CleaningWind(Modelname,Location,Ecos):
    UAS=pd.read_csv(Location+fnmatch.filter(os.listdir(Location),"uas_"+Modelname+"*")[0])
    VAS=pd.read_csv(Location+fnmatch.filter(os.listdir(Location),"vas_"+Modelname+"*")[0])
    list(UAS.columns.values)
    Timestep=UAS["Timestep"]
    WD_Out=Timestep
    WS_Out=Timestep
    
    for eco in Ecos:
        UAS_one=UAS[eco]
        VAS_one=VAS[eco]
        ##Pythag 
        WS=np.sqrt((VAS_one**2)+(UAS_one**2))
        
        ##Here is my test to validate 
        ##VAS is northward wind
        ## UAS is Eastward Wind
        ## So the order should be Southward, Northward, Westward, Eastward
        #VAS_try=np.array([-1,1,0,0])
        #UAS_try=np.array([0,0,-1,1])
        ## Results should there be 0(northorgin),180(southorigin),
        ## 90(east origin),270(west origin)4
        #test=np.mod(180+np.rad2deg(np.arctan2(UAS_try, VAS_try)),360)
        WD=np.mod(180+np.rad2deg(np.arctan2(UAS_one, VAS_one)),360)
        
        WD_Out = pd.concat([WD_Out.reset_index(drop=True), WD], axis=1)
        WS_Out= pd.concat([WS_Out.reset_index(drop=True), WS], axis=1)
    
    zeros=pd.DataFrame(np.zeros([34332,20]))
    WD_Out = pd.concat([WD_Out.reset_index(drop=True), zeros], axis=1)
    WS_Out= pd.concat([WS_Out.reset_index(drop=True), zeros], axis=1)
    
    WD_Out.columns=list(UAS.columns.values)[1:32]
    WS_Out.columns=list(UAS.columns.values)[1:32]
    
    WDname=Location+"direction_Wind_"+Modelname+"rcp85_r1i1p1_rcp85_2006_2099_CONUS.csv"
    WSname=Location+"speed_Wind_"+Modelname+"rcp85_r1i1p1_rcp85_2006_2099_CONUS.csv"
    WD_Out.to_csv(WDname)
    WS_Out.to_csv(WSname)
    os.remove(Location+fnmatch.filter(os.listdir(Location),"uas_"+Modelname+"*")[0]) 
    os.remove(Location+fnmatch.filter(os.listdir(Location),"vas_"+Modelname+"*")[0]) 


### End Wind 


def WriteitLikeLandis(Drive,Location,Ecoregionnumber,Modelname): 
    Importfiles=[f for f in os.listdir(Location) if Modelname in f]
    print("Found files to be written in LANDIS text:")
    print(Importfiles)
    
    pr_units=list(np.repeat(["MEAN(mm/d)","VARIANCE(mm/d^2)","STD_DEV(mm/d)"],repeats=Ecoregionnumber))
    temp_units=list(np.repeat(["MEAN(CELSIUS)","VARIANCE(CELSIUS^2)","STD_DEV(CELSIUS)"],repeats=Ecoregionnumber))
    rh_units=list(np.repeat(["MEAN(%)","VARIANCE(%^2)","STD_DEV(%)"],repeats=Ecoregionnumber))
    windspeed_units=list(np.repeat(["MEAN(m/s)","VARIANCE(m/s^2)","STD_DEV(m/s)"],repeats=Ecoregionnumber))
    winddirec_units=list(np.repeat(["MEAN(DEGREES)","VARIANCE(DEGREES^2)","STD_DEV(DEGREES)"],repeats=Ecoregionnumber))
    print("Formatting Dictionary")
    file=[]
    ##Start loop here    
    dictionary_df=dict()
    for file in Importfiles: 
        print(file)
        dt=pd.read_csv(Location+file)
        try:
            dt=dt.drop(columns=['Unnamed: 0'])
        except: 
            print("no column to remove")
        dt['Timestep']=dt['Timestep']+"T00:00:00Z"
        
        first_variable_letter=file[0]
        #print(first_variable_letter)
        if (first_variable_letter == "t"):
          units_vec=temp_units
          if "tasmax" in file: 
             hashname= "#maxtemp"
          if "tasmin" in file: 
             hashname= "#mintemp"
        if (first_variable_letter == "r"):
          units_vec=rh_units
          if "rhsmin" in file: 
             hashname= "#maxrh"
          if "rhsmax" in file: 
             hashname= "#minrh"
        if (first_variable_letter == "p"):
            units_vec=pr_units
            hashname="#ppt"
        
        if (first_variable_letter == "d"):
          units_vec=winddirec_units
          hashname="#windirection"
          
        if (first_variable_letter == "s"):
          units_vec=windspeed_units
          hashname="#windspeed"
        #print(units_vec)
        column_names=["TIMESTEP"]
        for i in units_vec:
            column_names.append(str(i))
        #print(column_names)
        #print(hashname)
        dt.columns=column_names
        dictionary_df[hashname]=dt
    
    print("Writing to output")
    with open(Drive+"Outputs/"+Modelname+"LANDIS_input.csv","w") as f:
            for key in dictionary_df:
                #print(key)
                f.write(key+"\n")   
                
                onedata=dictionary_df[key]
                #len(onedata)
                cols=onedata.columns.tolist()
                for t in cols:
                    f.write(t+",")
                f.write("\n")
                for i in list(range(0,len(onedata),5000)):  
                 #   print(i)
                #i=0
                    onedatashort=onedata.iloc[i:i+5000]
                    #onedatashort=onedata.iloc[1:10]
                    x = onedatashort.to_string(header=False,
                                  index=False,
                                 index_names=False).split('\n')
                    var= [','.join(ele.split()) for ele in x]
                #   wr = csv.writer(file1, quoting=csv.QUOTE_ALL)
                    for line in var:
                        #print(str(var))
                        f.write(str(line)+'\n')