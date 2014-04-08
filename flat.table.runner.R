#########################################################################################
## Running
#########################################################################################
source("~/makeFlatTables.v2.R")

I <- list(name="/user/sguha/fhr/samples/output/fromjson1pct",tag=FALSE)
## I <- list(name="/user/sguha/fhr/samples/output/5pct",tag=TRUE)
I <- list(name="/fhr/" ,tag=FALSE)

rhread("/user/sguha/fhr/samples/output/createdTime.txt",type='text')
fileOrigin <-  strsplit(rhread("/user/sguha/fhr/samples/output/createdTime.txt",type='text'),"\t")[[2]]
fileOrigin <-  strftime(fileOriginDate <- as.Date(strsplit(fileOrigin,"_")[[1]][[2]]),"%Y%m%d")
ASOF <- strftime(fileOriginDate-7,"%Y-%m-%d")
rhmkdir(sprintf("/user/sguha/rollups/%s-1pct",ASOF))
hdfs.setwd(sprintf("/user/sguha/rollups/%s-1pct/",ASOF))


timeChunksDay <- dayTimeChunk(strftime(fileOriginDate-170,"%Y-%m-%d"),strftime(fileOriginDate-7,"%Y-%m-%d"))
uday <- rhwatch(map=m, reduce=rhoptions()$temp$colsummer
                ,input=I$name
                ,param=list(whichDate=fileOrigin,timeChunks=timeChunksDay,needstobetagged=I$tag)
                ,jobname="day",mon.sec=0
                ,debug='count' ,output="rday",read=FALSE)

timeChunksWk <- weekTimeChunk(strftime(fileOriginDate-170,"%Y-%m-%d"),strftime(fileOriginDate-7,"%Y-%m-%d"))
uwk <- rhwatch(map=m, reduce=rhoptions()$temp$colsummer
               ,input=I$name
               ,param=list(whichDate=fileOrigin,timeChunks=timeChunksWk,needstobetagged=I$tag)
               ,jobname="week",mon.sec=0
               ,debug='count' ,output="rweek",read=FALSE)

timeChunksMon <- monthTimeChunk(strftime(fileOriginDate-170,"%Y-%m-%d"),strftime(fileOriginDate-7,"%Y-%m-%d"))
uwk <- rhwatch(map=m, reduce=rhoptions()$temp$colsummer
               ,input=I$name
               ,param=list(whichDate=fileOrigin,timeChunks=timeChunksMon,needstobetagged=I$tag)
               ,jobname="month",mon.sec=0
               ,debug='count' ,output="rmonth",read=FALSE)

timeChunksQtr <- quarterTimeChunk(c(2013,2014))
uwk <- rhwatch(map=m, reduce=rhoptions()$temp$colsummer
               ,input=I$name
               ,param=list(whichDate=fileOrigin,timeChunks=timeChunksQtr,needstobetagged=I$tag)
               ,jobname="qtr",mon.sec=0
               ,debug='count' ,output="rqtr",read=FALSE)

################################################################################
## convert to text    
################################################################################
toText <- function(i,o){
    y <- rhwatch(map=function(a,b){    rhcollect(NULL, c(a,b))    },reduce=0, input=i
                 ,output=rhfmt(type='text', folder=o,writeKey=FALSE,field.sep="\t",stringquote=""),read=FALSE)
    a <- rhls(o)$file
    rhdel(a[!grepl("part-",a)])
    rhchmod(o,"777")
    o
}

toText("rday",o="tday")
toText("rweek",o="tweek")
toText("rmonth",o="tmonth")
toText("rqtr",o="tqtr")



## The dimensions are (all characters)
## vendor      : geckoAppInfo$vendor
## prodname    : geckoAppInfo$name
## channel     : geckoAppInfo$updateChannel
## arch        : data$last$org.mozilla.sysinfo.sysinfo$architecture
## osname      : data$last$ org.mozilla.sysinfo.sysinfo$name
## osversion   : data$last$ org.mozilla.sysinfo.sysinfo$version
## locale      : data$last$ org.mozilla.appInfo.appinfo$locale
## geo
## timeStart   : for daily, the day. For weekly the start of the week, for monthly, the start of the month and so on
## timeEnd     : the end
## prodversion : the version the profile was on, can also be "ANY" which ibasically ignores version
## snapshot date: YYYYMMDD
## Missing values are given "missing" (some variables,e.g. prodname, are also "")

## The measures are

## tTotalProfiles         : total number of profiles at the beginning of the time period (tp) 
## tNewProfiles           : total new profiles created during the time period
## tExistingProfiles      : total profiles that were created before the time period
## tTotalAge              : sum of profile age
## tInactiveLast30Days    : # of  profiles inactive for 30 days before the start of the period (could be defined differently)
## tActiveProfiles        : # of active profiles
## tActiveCleanSec        : sum of active clean seconds
## tActiveAbortedSec      : sum of aborted seconds
## tTotalCleanSec         : sum of total clean seconds
## tTotalAbortedSec       : sum of total aborted seconds
## tCleanSessions         : # of clean sessions
## tAbortedSessions       : # of aborted sessions
## tSessions              : # of sessions
## tDays                  : # of days
## tSrchGoogleUrlBar      : # of google urlbar searches(regex== "google[a-zA-Z0-9._-]*.urlbar")
## tSrchGoogleContextMenu : "google[a-zA-Z0-9._-]*.contextmenur
## tSrchGoogleSearchBar   : "google[a-zA-Z0-9._-]*.searchbar"
## tSrchGoogleAboutHome   : "google[a-zA-Z0-9._-]*.abouthome"
## tSrchGoogle            : "google"
## tSrchYahooUrlBar       : 
## tSrchYahooContextMenu  : 
## tSrchYahooSearchBar    : 
## tSrchYahooAboutHome    : 
## tSrchYahoo             : 
## tSrchBingUrlBar        : 
## tSrchBingContextMenu   : 
## tSrchBingSearchBar     : 
## tSrchBingAboutHome     : 
## tSrchBing              : 
## tSrchOthersUrlBar      : 
## tSrchOthersContextMenu : 
## tSrchOthersSearchBar   : 
## tSrchOthersAboutHome   : 
## tSrchOthers            : 
## tMainms                : sum of main (milliseconds)
## tFPms                  : sum of firstpaint (ms)
## tSRms                  : sum of sessionrestore (ms)
## tExtensions            : number of active extensions
## tPlugins               : 
## tThemes                : 
## tCrashPending          : sum of crash pending
## tCrashSubmit           : sum of crash submit
## tCrashMain             : sum of crash main (see https://hg.mozilla.org/mozilla-central/file/0086975029c3/services/healthreport/docs/dataformat.rst#l1041)
## tBookmarks             : sum of bookmarks
## tPages                 : sum of pages visited
## tNewPages              : sum of new pages visited in time period (some summands can be negative)
## tNewBookmarks          : sum of new bookmarks (some summands can be negative)
## tSyncDevices           : # of syncdevices
## tSyncEnabled           : # of profiles with sync enabled
## tUpdateEnabled         : # of profiles with update enbled
## tTelemetry             : # of profiles with telemetry on (more than half the days in time period have telemetry on) (if profile is not active in period, use Last Value Carried Forward)
## tDefault               : # of profile with default on (if profile was not active in period, use Last Value Carried Forward)








## IMPALA commands on peachgw
## impala-shell
## connect node1.peach.metrics.scl3.mozilla.com;

## drop table fhrweekly1pct;
## drop table xtemp;
## create external table xtemp(rangeStart string,version string,build string,vendor string,prodname  string,channel  string,arch  string,osname  string,osversion  string,locale  string,geo  string,nActiveProfiles double,existingprofile double,newProfile  double,isDefault  double,isTelemOn  double,nActiveCleanSec  double,nActiveAbortedSec  double,nTotalCleanSec  double,nTotalAbortedSec  double,nCleanSessions  double,nAbortedSessions  double,srchGoogle  double,srchBing  double,srchYahoo  double,srchOthers double,nMain  double,nFP  double,nSR  double,nExtensions  double,nPlugins  double,nThemes  double,nCrashPending  double,nCrashSubmit  double,nBookmarks  double) row format delimited fields terminated by '\t' ;
## ## LOAD DATA INPATH '/user/sguha/weeklytxt/'  into table xtemp;
## ALTER TABLE fhrweekly LOCATION '/user/sguha/weeklytxt/'; 
## create table  fhrweekly1pct LIKE xtemp STORED AS PARQUET;
## insert overwrite table fhrweekly1pct  select * from xtemp;
## compute stats fhrweekly1pct;

## drop table fhrdaily;
## drop table xtemp;
## create table xtemp(rangeStart string,version string,build string,vendor string,prodname  string,channel  string,arch  string,osname  string,osversion  string,locale  string,geo  string,nActiveProfiles double,existingprofile double,newProfile  double,isDefault  double,isTelemOn  double,nActiveCleanSec  double,nActiveAbortedSec  double,nTotalCleanSec  double,nTotalAbortedSec  double,nCleanSessions  double,nAbortedSessions  double,srchGoogle  double,srchBing  double,srchYahoo  double,srchOthers double,nMain  double,nFP  double,nSR  double,nExtensions  double,nPlugins  double,nThemes  double,nCrashPending  double,nCrashSubmit  double,nBookmarks  double) row format delimited fields terminated by '\t';
## LOAD DATA INPATH '/user/sguha/dailytxt/'  into table xtemp;
## create table  fhrdaily1pct LIKE xtemp STORED AS PARQUET;
## insert overwrite table fhrdaily1pct  select * from xtemp;
## compute stats fhrdaily1pct;





