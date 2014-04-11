eval(odbc)

## > w[grepl("rollup",w)]
## [1] "fhr_rollups_day"   "fhr_rollups_month" "fhr_rollups_qtr"   "fhr_rollups_week" 
hdfs.setwd("/user/sguha/rollups/2014-03-24-1pct")

a <- rhwatch(map=function(a,r){
    rhcollect(sample(SLICE, 1), as.data.table(append(a,as.list(r))))
}, reduce=rhoptions()$templ$rbinder(),input="rmonth", output="/user/sguha/tmp/d1",param=list(SLICE=1:1000),setup=expression({suppressPackageStartupMessages(library(data.table))}))
a <- rbindlist(lapply(a,"[[",2))

################################################################################
## From david: the number of unique active profiles from Japan during the month of March 2014
################################################################################
rhwatch(map=function(a,b){
    if(b$geo=="JP" & any( names(b$data$days)>="2014-03-01" & names(b$data$days)<="2014-03-31"))
        rhcounter("a","jp",1)
},reduce=summer, input="/user/sguha/fhr/samples/output/fromjson1pct", debug='count')
adhoc("select sum(tActiveProfiles) as activeProfiles from fhr_rollups_month where timeStart='2014-03-01' and geo='JP' and prodVersion='ANY'", conn)
a[prodversion=='ANY' & timeStart=="2014-03-01" & geo=='JP', list(activeProfiles=sum(tActiveProfiles)  ,tTotalProfiles=sum(tTotalProfiles))]

################################################################################
## From an email in which Chad asked, number of Nightly and Aurora profiles
################################################################################
adhoc("select channel, sum(tActiveProfiles) as tActiveProfiles ,sum(tTotalProfiles) as tTotalProfiles from fhr_rollups_month where timeStart='2014-03-01'  and prodVersion='ANY' and  channel in ('nightly','aurora') group by channel", conn)

a1 <- rhwatch(map=function(a,b){
    b <- fromJSON(b)
    if( b$geckoAppInfo$updateChannel %in% c("nightly","aurora"))
        rhcollect(b$geckoAppInfo$updateChannel,1)
},reduce=summer, input=sqtxt("/user/sguha/fhr/samples/output/1pct"), debug='count',setup=expression({library(rjson)}))

a[prodversion=='ANY' & timeStart=="2014-03-01" & channel %in% c("nightly","aurora"), list(activeProfiles=sum(tActiveProfiles)  ,tTotalProfiles=sum(tTotalProfiles)),by=channel]

ts <- rhwatch(map=function(a,r){
    y <- strsplit(r, "\t")[[1]]
    if(y[12]=="ANY" && y[10] == "2014-03-01" && y[3] %in% c("nightly","aurora")){
        rhcollect(y[3], as.numeric( c(activeProfiles = y[18], tTotalProfiles = y[13])))
    }
},reduce=rhoptions()$temp$colsummer, input=rhfmt("tmonth",type='text'))
        

