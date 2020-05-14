###Author: Kate Jones
###Date: 04/08/2020
###What: This is one of several scripts used to process MACA climate data for LANDIS Simulations. This is the final
######## script that ingests outputs from python scripts created by ZJR.

###Notes: This assumes all csvs for a given climate are in a folder, named by the climate model (designated below - "modelfolder").

library(data.table)

#what is the name of the folder containing the climate files for a given climate model? 
modelfolder="mri"

#Designate the output filename
climfilename="MRI-CGCM3rcp85_LANDIS.csv"

#rimestepead in full file paths and csv names for trimming to make dataframe names
file_list<-list.files(paste0("C:\\Users\\thebrain\\Dropbox\\DEAL_lab\\climate_work\\SApps_climatecsv\\all_models\\",modelfolder),recursive=F,full.names = TRUE, include.dirs=FALSE)

#for automation purposes, when reading this in...could search for the 
#beginning of the wind direction/wind speed files and replace them with 
#speedwind and #directionwind, which I did to make the climate variable 
#matching easier by the unique first letter of the file/clim variable

# i also simply deleted uas and vas from the file list, 
#since I didn't want to read those in...could automate this (i.e. drop from file list once read in)
climate_vars<-list.files(paste0("C:\\Users\\thebrain\\Dropbox\\DEAL_lab\\climate_work\\SApps_climatecsv\\all_models\\",modelfolder),recursive=F,include.dirs=FALSE)



#make a list of variable names (ie. "pr", "tmin", etc.)
var_names<-list()
for (n in 1:length(climate_vars)){
  
  var_names[n]<-strsplit(climate_vars[n],"_")[[1]][1]
}


#create vectors of mean, variance, std_dev for each climate variable
pr_units<-rep(c("MEAN(mm/d)","VARIANCE(mm/d^2)","STD_DEV(mm/d)"),times=c(10,10,10))
temp_units<-rep(c("MEAN(CELSIUS)","VARIANCE(CELSIUS^2)","STD_DEV(CELSIUS)"),times=c(10,10,10))
rh_units<-rep(c("MEAN(%)","VARIANCE(%^2)","STD_DEV(%)"),times=c(10,10,10))
windspeed_units<-rep(c("MEAN(m/s)","VARIANCE(m/s^2)","STD_DEV(m/s)"),times=c(10,10,10))
winddirec_units<-rep(c("MEAN(DEGREES)","VARIANCE(DEGREES^2)","STD_DEV(DEGREES)"),times=c(10,10,10))

#column_names<-c("TIMESTEP",ecoregion_vec)
df_list<-list()
#read in variable dataframes
for (i in 1:length(file_list)){
  
  dt<-fread(file_list[i])
  
#get rid of first counting column
  dt<-dt[,-1]

#pr$Timestep comes in as string!

#put the time in LANDIS accepted format
  dt$Timestep_str<-paste0(dt$Timestep,"T00:00:00Z")

#to maintain same number of cols, set newly created column as previous time column, then delete new time columns
  dt$Timestep<-dt$Timestep_str
  dt$Timestep_str<-NULL
  
  
  #do some column stuff based on the variable name in this loop (from cliamte vars)
  first_variable_letter<-strsplit(var_names[[i]],"")[[1]][1]

  
  if (first_variable_letter == "t"){
    units_vec=temp_units
  }
  
  if (first_variable_letter == "r"){
    units_vec=rh_units
  }
  
  if (first_variable_letter == "p"){
    units_vec=pr_units
  }
  
  if (first_variable_letter == "d"){
    units_vec=winddirec_units
  } 
  
  if (first_variable_letter == "s"){
    units_vec=windspeed_units
  }
  
  column_names<-c("TIMESTEP",units_vec)
  print(column_names)
  
  colnames(dt)<-column_names
  
  #assing the variable name from split to the dataframe as its variable name
  df_list[[i]]<-assign(as.character(var_names[[i]]), dt, envir= .GlobalEnv)


}


##this writes them to the csv in whatever order they come in in the file list, will want to readjust this, me thinks.d


#after every loop in df_list make another write table command that appends a row with #whatever climate variable and na's
#and then another row of all ecoregion vectors

##need to be in order of file list - could alphabatize on entry to make this easier??
clim_vars_notation<-c("#winddirection","#ppt","#maxrh","#minrh","#windspeed", "#maxtemp","#mintemp")
blank_vec<-rep("",times=30)

#create vector of repeated ecoregion names will assign these as columns to each dataframe with check.names=FALSE
ecoregion_vec<-rep(c("eco2","eco3","eco4","eco5","eco6","eco7","eco8","eco9","eco10","eco11"), times = 3)
ecoreg_row<-c("",ecoregion_vec)
eco_t <- as.matrix(t(ecoreg_row))

clim_row<-c(clim_vars_notation[1],blank_vec)
clim_t <- as.matrix(t(clim_row))


init<-data.frame()
init[1,1:31]<-clim_t
init[2,1:31]<-eco_t
#get it initialized without overwriting, need to create this each time
write.table(init,paste0("C:\\Users\\thebrain\\Dropbox\\DEAL_lab\\climate_work\\SApps_climatecsv\\",climfilename),sep=",",col.names=FALSE,row.names=FALSE)

#writing rows to newly created csv
for(i in seq_along(df_list)){
  print(i)
  
  write.table(
    df_list[[i]],
    paste0("C:\\Users\\thebrain\\Dropbox\\DEAL_lab\\climate_work\\SApps_climatecsv\\",climfilename),
    append    = TRUE,
    sep       = ",",
    row.names = FALSE,
    col.names = TRUE,
  )
  
  
  clim_row<-c(clim_vars_notation[i+1],blank_vec)
  clim_t <- as.matrix(t(clim_row))
  
  
  add<-data.frame()
  add[1,1:31]<-clim_t
  add[2,1:31]<-eco_t
  
  #get it initialized without overwriting, nead to create this each time
  write.table(add,paste0("C:\\Users\\thebrain\\Dropbox\\DEAL_lab\\climate_work\\SApps_climatecsv\\",climfilename),sep=",",col.names=FALSE,row.names=FALSE,append=TRUE)
  
  
}

##### NEED TO WRITE IN A QUICK DELETION OF THE LAST ROW

