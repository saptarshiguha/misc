## Some dimensions are additive. A profile that is tagged as Windows forever cannot lie in OS X. Hence were we
## to condition on OS, then adding up 'ActiveProfiles' across all partitions does not double count since the
## partitions are truly partitions. But not all dimensions are additive. Cconsider a dimension that can change
## over time for example, 'Version'. A profile cannot be partitioned into (os, version) since that profile can
## lie in multiple (os,version) tuples. Had we a count of active profiles in (os,version) tuples, then adding
## willy nilly say across versions would give us a double count. Essentially any dimension that can change
## over time would lead to a double count. To get an accurate number one needs to filter for one value of a
## 'temporal dimension'
##
## temporal dimensions are: version, buildid, wasActive and isDefault (the last two i'm inclined to drop)
##
## But this makes for near impossible queries. Consider the following simple query: Number of Existing
## Profiles as of November beginning?  If our counts are conditioned on OS, Vendor, Channel, Arch and Version
## then "total exisiting != sum(existing) as all from table" because profiles that belonged to two or more
## versions would get double counted.  this is not a problem for daily( temporal dimensions change over a
## large time period, not a day) and less severe for a week, but still present even in that.  This is
## essentialy the ADI --> Uniques for a Month problem reworded. In this case, the profile is conditioned on
## day but the same profile can belong to multiple days.
##
## The only way out is to have seperate tables for temporal dimensions or have joins across tables ...
## i.e. one table for all profiles and their 'fixed' information and other tables with time changing
## information e.g. things in data$days.

wofYear <- function(b){
    ## b is a date
    bp <- as.POSIXlt(b)
    julian = bp$yday+1
    dow = bp$wday
    dowJan1 = as.POSIXlt(sprintf("%s-01-01", bp$year+1900))$wday
    weekNum = floor(((julian + 6) / 7)   )
    return (weekNum)
}
getDimensions <- function(b){
    list(vendor = isn(b$geckoAppInfo$vendor,"missing")
         ,prodname = isn(b$geckoAppInfo$name, "missing")
         ,channel = isn(b$geckoAppInfo$updateChannel, "missing")
         ,arch = isn(b$data$last$org.mozilla.sysinfo.sysinfo$architecture,"missing")
         ,osname = isn(b$data$last$ org.mozilla.sysinfo.sysinfo$name, "missing")
         ,osversion = isn(b$data$last$ org.mozilla.sysinfo.sysinfo$version, "missing")
         ,locale = isn(b$data$last$ org.mozilla.appInfo.appinfo$locale, "missing")
         ,geo = b$geo)
}
m <- function(a,b){
    ## we assume b has been jsonified and the appbuild/version suitably populated which is why i use fromjson1pct as the
    ## input sample. Using other samples/sources implies the code has to be modified to sort and fill in missing values
    if(needstobetagged){
        b <- fromJSON(b)
        b$data$days <- tagDaysByBuildVersion(b)
    }
    st1 <- Sys.time()
    bdim <- getDimensions(b)
    bdim$snapshot <- whichDate
    ## bdim$id <- a
    if(length(bdim)==0) return()
    profileCrDate <- strftime(  as.Date(b$data$last$org.mozilla.profile.age$profileCreation,"1970-01-01"), "%Y-%m-%d")
    for( timeChunk in timeChunks){
        days <- b$data$days [ names(b$data$days)>=timeChunk['start']  & names(b$data$days)<= timeChunk['end']]
        bdim$timeStart <- as.character(timeChunk['start'])
        bdim$timeEnd <- as.character(timeChunk['end'])
        ## This is computable for every profile whether they were active in the timeChunk or not
        tTotalProfiles <- 1
        tNewProfiles <- if(profileCrDate>= timeChunk['start']  && profileCrDate <= timeChunk['end']) 1 else 0
        tExistingProfiles <- if(profileCrDate< timeChunk['start']) 1 else 0
        tTotalAge <- isn(b$data$last$org.mozilla.profile.age$profileCreation,0)
        tInactiveLast30Days <- 1-any(names(b$data$days)>=strftime(as.Date(timeChunk['start'])-30,"%Y-%m-%d") & names(b$data$days)<timeChunk['start'])*1
        ## If they were inactive in the timeChunk, we need to contribute zero or some correct value for them
        if(length(days)==0){
            ## compute other dimensions from their last present value: defaultStatus, version and build
            pastDays <- b$data$days[ rev(names(b$data$days)<timeChunk['start']) ];X <- NULL
            ## bdim$wasActive <- 0

            Xtelem <- Xdefault <- Xversion <- Xbuild <- NULL
            for(i in pastDays){
                if(!is.null(i$org.mozilla.appInfo.appinfo$isDefaultBrowser) && is.null(Xdefault)){
                    Xdefault <- 1*(i$org.mozilla.appInfo.appinfo$isDefaultBrowser)
                }
                if(!is.null(i$org.mozilla.appInfo.appinfo$isTelemetryEnabled) && is.null(Xtelem)){
                    Xtelem <- 1*(i$org.mozilla.appInfo.appinfo$isTelemetryEnabled)
                }
                if(!is.na(i$buildInfo$version[1]) && !is.null(i$buildInfo$version[1]) && is.null(Xversion)){
                    Xversion <- i$buildInfo$version[1]
                }
                if(!is.na(i$buildInfo$build[1]) && !is.null(i$buildInfo$build[1]) && is.null(Xbuild)){
                    Xbuild <- i$buildInfo$build[1]
                }
                if(!is.null(Xtelem) && !is.null(Xdefault) && !is.null(Xversion) && !is.null(Xbuild)) break
            }
            ## bdim$defaultStatus <- isn(Xdefault,0)      ## it was never found!
            ## bdim$telemetryStatus <- isn(Xtelem,0)      ## it was never found!
             ## it was never found!
            ## bdim$prodbuild <- isn(Xbuild,"missing")     ## it was never found!

            tActiveProfiles <- tActiveCleanSec <- tActiveAbortedSec <- tTotalCleanSec <- 0
            tTotalAbortedSec <- tCleanSessions <- tAbortedSessions <- tDays <- tSessions <- 0
            tSrchGoogleUrlBar <- tSrchGoogleContextMenu <- tSrchGoogleSearchBar <- tSrchGoogleAboutHome <- tSrchGoogle <- 0
            tSrchYahooUrlBar <- tSrchYahooContextMenu <- tSrchYahooSearchBar <- tSrchYahooAboutHome <- tSrchYahoo <- 0
            tSrchBingUrlBar <- tSrchBingContextMenu <- tSrchBingSearchBar <- tSrchBingAboutHome <- tSrchBing <- 0
            tSrchOthersUrlBar <- tSrchOthersContextMenu <- tSrchOthersSearchBar <- tSrchOthersAboutHome <- tSrchOthers <- 0
            tMainms <-tFPms <- tSRms <- tExtensions <- tPlugins <- tThemes <- 0
            tCrashPending <- tCrashSubmit <- tCrashMain <- tBookmarks <- tPages <- tNewPages <- tNewBookmarks <- 0
            tSyncDevices <- tSyncEnabled <- tUpdateEnabled <- 0
            tTelemetry <-isn(Xtelem,0); tDefault <- isn(Xdefault,0) 
            rhcounter("s","0",1)
            for(pd in list(isn(Xversion,"missing"),"ANY")){
                bdim$prodversion <-pd
                rhcollect(bdim, c(tTotalProfiles=tTotalProfiles,tNewProfiles=tNewProfiles,tExistingProfiles=tExistingProfiles
                              ,tTotalAge=tTotalAge,tInactiveLast30Days=tInactiveLast30Days
                              ,tActiveProfiles=tActiveProfiles,tActiveCleanSec=tActiveCleanSec,tActiveAbortedSec=tActiveAbortedSec,tTotalCleanSec=tTotalCleanSec
                              ,tTotalAbortedSec=tTotalAbortedSec,tCleanSessions=tCleanSessions,tAbortedSessions=tAbortedSessions,tSessions=tSessions,tDays=tDays
                              ,tSrchGoogleUrlBar=tSrchGoogleUrlBar,tSrchGoogleContextMenu=tSrchGoogleContextMenu,tSrchGoogleSearchBar=tSrchGoogleSearchBar
                              ,tSrchGoogleAboutHome=tSrchGoogleAboutHome,tSrchGoogle=tSrchGoogle
                              ,tSrchYahooUrlBar=tSrchYahooUrlBar,tSrchYahooContextMenu=tSrchYahooContextMenu
                              ,tSrchYahooSearchBar=tSrchYahooSearchBar,tSrchYahooAboutHome=tSrchYahooAboutHome,tSrchYahoo=tSrchYahoo
                              ,tSrchBingUrlBar=tSrchBingUrlBar,tSrchBingContextMenu=tSrchBingContextMenu,tSrchBingSearchBar=tSrchBingSearchBar,tSrchBingAboutHome=tSrchBingAboutHome
                              ,tSrchBing=tSrchBing
                              ,tSrchOthersUrlBar=tSrchOthersUrlBar,tSrchOthersContextMenu=tSrchOthersContextMenu,tSrchOthersSearchBar=tSrchOthersSearchBar
                              ,tSrchOthersAboutHome=tSrchOthersAboutHome,tSrchOthers=tSrchOthers
                              ,tMainms=tMainms,tFPms=tFPms,tSRms=tSRms,tExtensions=tExtensions,tPlugins=tPlugins
                              ,tThemes=tThemes,tCrashPending=tCrashPending,tCrashSubmit=tCrashSubmit,tCrashMain=tCrashMain,tBookmarks=tBookmarks,tPages=tPages
                                  ,tNewPages=tNewPages,tNewBookmarks=tNewBookmarks,tSyncDevices=tSyncDevices,tSyncEnabled=tSyncEnabled,tUpdateEnabled=tUpdateEnabled
                                  ,tTelemetry = tTelemetry, tDefault=tDefault))
            }
        }else{
            rhcounter("s","1",1)
            ## So they were active now, we need to compute the t* variables across days on same build, and version
            ## in this case, their default status(telemetry status) is the average default status(telemetry)>0.5
            telem <- sum(unlist(lapply(days,function(s) s$org.mozilla.appInfo.appinfo$isTelemetryEnabled)))
            ## bdim$defaultStatus <- 1*(telem > 0.5*length(days)) ## if not present, 0
            default <- sum(unlist(lapply(days,function(s) s$org.mozilla.appInfo.appinfo$isDefaultBrowser)))
            ## bdim$telemetryStatus <- 1*(telem > 0.5*length(days)) ## if not present,0

            version <- as.character(unlist(lapply(days,function(s) if(is.na(s$buildInfo$version)) "missing" else s$buildInfo$version)))
            ## build   <- as.character(unlist(lapply(days,function(s) if(is.na(s$buildInfo$build)) "missing" else s$buildInfo$build)))
            L <-  split(days, list(version),drop=TRUE)
            L[[ length(L)+1 ]] <- days
            ## bdim$wasActive <- 0
            for( .i in seq_along(L)){
                setOfDays <- L[[.i]]
                ## bdim$wasActive <- 1
                if(length(setOfDays) == 0) next
                if(.i < length(L))
                    bdim$prodversion <- if(is.na(setOfDays[[1]]$buildInfo$version)) "missing" else setOfDays[[1]]$buildInfo$version
                else
                    bdim$prodversion <- "ANY"
                ## bdim$prodbuild <- if(is.na(setOfDays[[1]]$buildInfo$build)) "missing" else setOfDays[[1]]$buildInfo$build

                tTelemetry <- 1*( sum(unlist(lapply(setOfDays,function(s) s$org.mozilla.appInfo.appinfo$isTelemetryEnabled))) > 0.5*length(setOfDays))
                tDefault <- 1*(sum(unlist(lapply(setOfDays,function(s) s$org.mozilla.appInfo.appinfo$isDefaultBrowser))) > 0.5*length(setOfDays))
                tActiveProfiles <- 1
                tActiveCleanSec <- sum(unlist(lapply(setOfDays, function(dc){ dc$org.mozilla.appSessions.previous$cleanActiveTicks * 5 })))
                tActiveAbortedSec <- sum(unlist(lapply(setOfDays, function(dc){ dc$org.mozilla.appSessions.previous$abortedActiveTicks * 5 })))
                tTotalCleanSec <- sum(unlist(lapply(setOfDays, function(dc){ dc$org.mozilla.appSessions.previous$cleanTotalTime * 5 })))
                tTotalAbortedSec <- sum(unlist(lapply(setOfDays, function(dc){ dc$org.mozilla.appSessions.previous$abotedTotalTime * 5 })))
                tCleanSessions <- length(unlist(lapply(setOfDays, function(dc){ dc$org.mozilla.appSessions.previous$cleanTotalTime  })))
                tAbortedSessions <- sum(unlist(lapply(setOfDays, function(dc){ dc$org.mozilla.appSessions.previous$abortedTotalTime  })))
                tSessions <- sum(unlist(lapply(setOfDays, function(dc) length(dc$org.mozilla.appSessions.previous$main))))
                tDays <- length(setOfDays)
                tSrchGoogle <-  sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("google",names(x))])})))
                tSrchGoogleUrlBar <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("google[a-zA-Z0-9._-]*.urlbar",names(x))])})))
                tSrchGoogleContextMenu <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("google[a-zA-Z0-9._-]*.contextmenu",names(x))])})))
                tSrchGoogleSearchBar <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("google[a-zA-Z0-9._-]*.searchbar",names(x))])})))

                tSrchGoogleAboutHome <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("google[a-zA-Z0-9._-]*.abouthome",names(x))])})))
                tSrchYahoo <-  sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("yahoo",names(x))])})))
                tSrchYahooUrlBar <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("yahoo[a-zA-Z0-9._-]*.urlbar",names(x))])})))
                tSrchYahooContextMenu <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("yahoo[a-zA-Z0-9._-]*.contextmenu",names(x))])})))
                tSrchYahooSearchBar <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("yahoo[a-zA-Z0-9._-]*.searchbar",names(x))])})))
                tSrchYahooAboutHome <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("yahoo[a-zA-Z0-9._-]*.abouthome",names(x))])})))
                tSrchBing <-  sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("bing",names(x))])})))
                tSrchBingUrlBar <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("bing[a-zA-Z0-9._-]*.urlbar",names(x))])})))
                tSrchBingContextMenu <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("bing[a-zA-Z0-9._-]*.contextmenu",names(x))])})))
                tSrchBingSearchBar <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("bing[a-zA-Z0-9._-]*.searchbar",names(x))])})))
                tSrchBingAboutHome <- sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[grepl("bing[a-zA-Z0-9._-]*.abouthome",names(x))])})))
                tSrchOthers <-  sum(unlist(lapply(setOfDays,function(dc){ x <- dc$org.mozilla.searches.counts; unlist(x[!grepl("(google|yahoo|bing)",names(x))])})))
                tSrchOthersUrlBar <- sum(unlist(lapply(setOfDays,function(dc)
                                                     { x <- dc$org.mozilla.searches.counts; unlist(x[!grepl("(google|yahoo|bing)[a-zA-Z0-9._-]*.urlbar",names(x))])})))
                tSrchOthersContextMenu <- sum(unlist(lapply(setOfDays,function(dc)
                                                          { x <- dc$org.mozilla.searches.counts; unlist(x[grepl("(google|yahoo|bing)[a-zA-Z0-9._-]*.contextmenu",names(x))])})))
                tSrchOthersSearchBar <- sum(unlist(lapply(setOfDays,function(dc)
                                                        { x <- dc$org.mozilla.searches.counts; unlist(x[grepl("(google|yahoo|bing)[a-zA-Z0-9._-]*.searchbar",names(x))])})))
                tSrchOthersAboutHome <- sum(unlist(lapply(setOfDays,function(dc)
                                                        { x <- dc$org.mozilla.searches.counts; unlist(x[grepl("(google|yahoo|bing)[a-zA-Z0-9._-]*.abouthome",names(x))])})))
                tMainms <- sum(unlist(lapply(setOfDays, function(dc){  sum(dc$org.mozilla.appSessions.previous$main) }   )))
                tFPms <- sum(unlist(lapply(setOfDays, function(dc){  sum(dc$org.mozilla.appSessions.previous$firstPaint) } )))
                tSRms <- sum(unlist(lapply(setOfDays, function(dc){  sum(dc$org.mozilla.appSessions.previous$sessionRestored) } )))
                tExtensions <- sum(unlist(lapply(setOfDays,function(dc) dc$org.mozilla.addons.counts$extension)))
                tPlugins <- sum(unlist(lapply(setOfDays,function(dc) dc$org.mozilla.addons.counts$plugin)))
                tThemes <- sum(unlist(lapply(setOfDays,function(dc) dc$org.mozilla.addons.counts$theme)))
                tCrashPending <- sum(unlist(lapply(setOfDays,function(dc) dc$org.mozilla.crashes.crashes$pending)))
                tCrashSubmit <-  sum(unlist(lapply(setOfDays,function(dc) dc$org.mozilla.crashes.crashes$submitted)))
                tCrashMain <-  sum(unlist(lapply(setOfDays,function(dc) dc$org.mozilla.crashes.crashes$mainCrash)))
                tBookmarks <- sum(unlist(lapply(setOfDays,function(dc) dc$org.mozilla.places.places$bookmarks)))
                tPages <- sum(unlist(lapply(setOfDays,function(dc) dc$org.mozilla.places.places$pages)))
                tNewBookmarks <- local({ a <- unlist(lapply(setOfDays,function(dc) dc$org.mozilla.places.places$bookmarks))
                                         if(is.null(a)) 0 else diff(range(a))})
                tNewPages <- local({ a <- unlist(lapply(setOfDays,function(dc) dc$org.mozilla.places.places$pages))
                                     if(is.null(a)) 0 else diff(range(a))})
                tSyncDevices <- sum(unlist(lapply(setOfDays,function(dc) {x <- dc$org.mozilla.sync.devices;x[["_v"]] <- NULL; sum(unlist(x))})))
                tSyncEnabled <- sum(unlist(lapply(setOfDays,function(dc) {dc$org.mozilla.sync.sync$enabled})))
                tSyncEnabled <- 1*(tSyncEnabled>0.5*length(days))
                tUpdateEnabled <- sum(unlist(lapply(setOfDays,function(dc) {dc$org.mozilla.appInfo.update$enabled})))
                tUpdateEnabled <- 1*(tUpdateEnabled>0.5*length(days))
                rhcollect(bdim, c(tTotalProfiles=tTotalProfiles,tNewProfiles=tNewProfiles,tExistingProfiles=tExistingProfiles
                                  ,tTotalAge=tTotalAge,tInactiveLast30Days=tInactiveLast30Days
                                  ,tActiveProfiles=tActiveProfiles,tActiveCleanSec=tActiveCleanSec,tActiveAbortedSec=tActiveAbortedSec,tTotalCleanSec=tTotalCleanSec
                                  ,tTotalAbortedSec=tTotalAbortedSec,tCleanSessions=tCleanSessions,tAbortedSessions=tAbortedSessions,tSessions=tSessions,tDays=tDays
                                  ,tSrchGoogleUrlBar=tSrchGoogleUrlBar,tSrchGoogleContextMenu=tSrchGoogleContextMenu,tSrchGoogleSearchBar=tSrchGoogleSearchBar
                                  ,tSrchGoogleAboutHome=tSrchGoogleAboutHome,tSrchGoogle=tSrchGoogle
                                  ,tSrchYahooUrlBar=tSrchYahooUrlBar,tSrchYahooContextMenu=tSrchYahooContextMenu
                                  ,tSrchYahooSearchBar=tSrchYahooSearchBar,tSrchYahooAboutHome=tSrchYahooAboutHome,tSrchYahoo=tSrchYahoo
                                  ,tSrchBingUrlBar=tSrchBingUrlBar ,tSrchBingContextMenu=tSrchBingContextMenu,tSrchBingSearchBar=tSrchBingSearchBar,tSrchBingAboutHome=tSrchBingAboutHome
                                  ,tSrchBing=tSrchBing
                                  ,tSrchOthersUrlBar=tSrchOthersUrlBar,tSrchOthersContextMenu=tSrchOthersContextMenu,tSrchOthersSearchBar=tSrchOthersSearchBar
                                  ,tSrchOthersAboutHome=tSrchOthersAboutHome,tSrchOthers=tSrchOthers
                                  ,tMainms=tMainms,tFPms=tFPms,tSRms=tSRms,tExtensions=tExtensions,tPlugins=tPlugins
                                  ,tThemes=tThemes,tCrashPending=tCrashPending,tCrashSubmit=tCrashSubmit,tCrashMain=tCrashMain,tBookmarks=tBookmarks,tPages=tPages
                                  ,tNewPages=tNewPages,tNewBookmarks=tNewBookmarks,tSyncDevices=tSyncDevices,tSyncEnabled=tSyncEnabled,tUpdateEnabled=tUpdateEnabled
                                  ,tTelemetry = tTelemetry, tDefault=tDefault))
            }
        }
    }
    ## rhcounter("stats","timePerRecord100x",round(100*(Sys.time() - st1),2))
}
    
                                   
dayTimeChunk <- function(fr, to){
    lapply(strftime(seq(from=as.Date(fr),to=as.Date(to),by=1),"%Y-%m-%d"),function(s){ c(start=s, end=s)})
}
weekTimeChunk <- function(fr,to){
    ## Sunday is the first day of a week For example, 25 Dec 2013 was a Wednesday and hence belonged to the week
    ## starting Sunday 21st if 'fr' is not a sunday it will be rewinded to the Sunday it belongs if 'to' is not a
    ## Saturday, it will be rewinded to the previous Saturday(end of week)
    sundaystart <- as.Date(fr)-as.POSIXlt(as.Date(fr))$wday
    weeks <- seq(from=sundaystart,length=floor(as.numeric((as.Date(to)-sundaystart)) / 7),by=7)
    m <-lapply(weeks,function(s) c(start=strftime(s,"%Y-%m-%d"), end=strftime(s+7-1,"%Y-%m-%d")))
    names(m) <- NULL;m
}
monthTimeChunk <- function(fr,to){
    ## a sad and sorry algorithm, but it does the job and is drop in the ocean of time the code that follows it will be
    ## if the fr is not on the first round it up to the next month if not the end of the month round to end of month
    ends <- list(31,c(28,29),31,30,31,30,31,31,30,31,30,31)
    J <- function(s) if(s<10) sprintf("0%s", s) else sprintf("%s",s)
    roundDown <- function(fr,NY=FALSE){
        if(as.POSIXlt(fr)$mday != 1 && NY){
            if(as.POSIXlt(fr)$mon<=10) ##mon is 0-11
                sprintf("%s-%s-01", as.POSIXlt(fr)$year+1900, J(as.POSIXlt(fr)$mon+2))
            else {
                sprintf("%s-01-01", as.POSIXlt(fr)$year+1900+1)
            }
        }else{
            ## Just to the beginning of themonth
            sprintf("%s-%s-01", as.POSIXlt(fr)$year+1900,J(as.POSIXlt(fr)$mon+1))
        }
    }
    roundUp <- function(to){
        if(! as.POSIXlt(to)$mday %in% ends[[as.POSIXlt(to)$mon+1]]){
            sprintf("%s-%s-%s", as.POSIXlt(to)$year+1900, J(as.POSIXlt(to)$mon+1),ends[[as.POSIXlt(to)$mon+1]][[1]] )
        }else  strftime(to,"%Y-%m-%d")
    }
    fr <- roundDown( as.Date(fr),NY=TRUE); to <- roundUp(as.Date(to))
    lapply(strsplit(unique(sapply(seq(as.Date(fr),as.Date(to), by=1),function(s){
        sprintf("%s|%s",roundDown(s), roundUp(s))})),"|",fixed=TRUE),function(s){
            c(start=s[[1]], end=s[[2]])})
}
quarterTimeChunk <- function(year){
    list(list(start=sprintf("%s-01-01",year),end=sprintf("%s-03-31",year)),
         list(start=sprintf("%s-04-01",year),end=sprintf("%s-06-30",year)),
         list(start=sprintf("%s-07-01",year),end=sprintf("%s-09-30",year)),
         list(start=sprintf("%s-10-01",year),end=sprintf("%s-12-31",year)))
}
    

