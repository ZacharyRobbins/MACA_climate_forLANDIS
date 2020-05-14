# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 18:06:00 2020

@author: zacha
"""

import os,fnmatch
import pandas as pd
import numpy as np

Drive="C:/Users/zacha/Documents/GitHub/MACA_climate_forLANDIS/4-Models/"

Model="CNRM-CM5"

Models=["CNRM-CM5","HadGEM2","IPSL-CM5A-MR","MRI-CGCM3"]
for Model in Models:
    fnmatch.filter(os.listdir(Drive),"uas_"+Model+"*")[0]
    UAS=pd.read_csv(Drive+fnmatch.filter(os.listdir(Drive),"uas_"+Model+"*")[0])
    VAS=pd.read_csv(Drive+fnmatch.filter(os.listdir(Drive),"vas_"+Model+"*")[0])
    list(UAS.columns.values)
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
    
    eco=Ecos[1]
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
    
    WDname=Drive+"WindDirection_"+Model+"rcp85_r1i1p1_rcp85_2006_2099_CONUS.csv"
    WSname=Drive+"WindSpeed_"+Model+"rcp85_r1i1p1_rcp85_2006_2099_CONUS.csv"
    
    WD_Out.to_csv(WDname)
    WS_Out.to_csv(WSname)
    
    #'uas_CNRM-CM5_r1i1p1_rcp85_2006_2099_CONUS.csv'