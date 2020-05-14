# -*- coding: utf-8 -*-
"""
Created on Mon May 11 13:36:00 2020

@author: zacha
"""
import os 
import pandas as pd
import numpy as np
import os,fnmatch
import pandas as pd
import numpy as np

Drive='C:/Users/zacha/Documents/GitHub/MACA_climate_forLANDIS/'
Location=(Drive+"Temp/")
Models="GFDL-ESM2M"   
ModelName=Models
Importfiles=[f for f in os.listdir(Location) if ModelName in f]
Ecos=['eco2',
 'eco3',
 'eco4',
 'eco5',
 'eco6',
 'eco7',
 'eco8',
 'eco9',
 'eco10',
 'eco11']

def CleaningWind(ModelName,Location,Ecos):
    UAS=pd.read_csv(Location+fnmatch.filter(os.listdir(Location),"uas_"+ModelName+"*")[0])
    VAS=pd.read_csv(Location+fnmatch.filter(os.listdir(Location),"vas_"+ModelName+"*")[0])
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
        VAS_try=np.array([-1,1,0,0])
        UAS_try=np.array([0,0,-1,1])
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
    
    WDname=Location+"direction_Wind_"+ModelName+"rcp85_r1i1p1_rcp85_2006_2099_CONUS.csv"
    WSname=Location+"speed_Wind_"+ModelName+"rcp85_r1i1p1_rcp85_2006_2099_CONUS.csv"
    WD_Out.to_csv(WDname)
    WS_Out.to_csv(WSname)
    os.remove(Location+fnmatch.filter(os.listdir(Location),"uas_"+ModelName+"*")[0]) 
    os.remove(Location+fnmatch.filter(os.listdir(Location),"vas_"+ModelName+"*")[0]) 


### End Wind 
Ecoregionnumber=10
def WriteitLikeLandis(Location,Ecoregionnumber,ModelName): 
    Importfiles=[f for f in os.listdir(Location) if ModelName in f]
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
        dt=pd.read_csv(Location+file)
        dt=dt.drop(columns=['Unnamed: 0'])
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
    with open(Location+ModelName+"LANDIS_input.csv","w") as f:
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

WriteitLikeLandis(Location,Ecoregionnumber,ModelName)
