library(data.table)
library(microbenchmark)
# Create Sales Data

numWeeks <- 500
numStores <- 10000 # 3e4
numRows <- numWeeks*numStores

# When on Promo, price is about 10 and volumeSales is about 50 (first half of weeks)
dataOnPromo <- data.frame( volumeSales=rnorm(n = numRows/2,mean = 50,sd = 3 ), 
                           price=rnorm(n = numRows/2,mean = 10,sd = 0.5 ), promo=1, 
                           storeId=rep(1:numStores,each=numWeeks/2),
                           weekId=rep(1:(numWeeks/2),times=numStores) )
# When off Promo, price is about 15 and volumeSales is about 10 (second half of weeks)
dataNonPromo <- data.frame( volumeSales=rnorm(n = numRows/2,mean = 10,sd = 2 ), 
                            price=rnorm(n = numRows/2,mean = 15,sd = 0.5 ), promo=0, 
                            storeId=rep(1:numStores,each=numWeeks/2),
                            weekId=rep((numWeeks/2 + 1):numWeeks,times=numStores))

dataTotal <- rbind(dataNonPromo, dataOnPromo)
dataTotal <- dataTotal[order(dataTotal$storeId,dataTotal$weekId),]


# Checking weeks of datasets
unique(dataOnPromo$weekId)
unique(dataNonPromo$weekId)

# List of stores
storesList <- unique(dataTotal$storeId)

# Calculate sum of VolumeSales by Store  (only for the first 100 stores)
system.time({
  sumVolume <- c()
  for (curStore in storesList[1:100]){
    curData <- dataTotal[dataTotal$storeId==curStore, ]
    sumVolume[curStore] <- 0
    for (i in 1:nrow(curData)){
      sumVolume[curStore] <- sumVolume[curStore] + curData$volumeSales[i]
    }
  }
})




# Same using aggregate
system.time( out1 <- aggregate(x = dataTotal$volumeSales, 
                               by = list(dataTotal$storeId), sum) )
microbenchmark(aggregate(x = dataTotal$volumeSales, 
                         by = list(dataTotal$storeId), sum),times = 10)

# Same using data.table
dataTotalDT <- as.data.table(dataTotal)
system.time( out2 <- dataTotalDT[,sum(volumeSales),by=.(storeId)] )
microbenchmark(dataTotalDT[,sum(volumeSales),by=.(storeId)])

# The same as above for quantile function
system.time( out1 <- aggregate(x = dataTotal$volumeSales, 
                               by = list(dataTotal$storeId), quantile,p=0.75) )

system.time( out2 <- dataTotalDT[,quantile(volumeSales,p=0.75),by=list(storeId)] )



# Linear regression per store (for loop, subsetting using data.frame "dataTotal")
t1 <- system.time({
  coef <- c()
  for (curStore in storesList[1:100]){
    curData <- dataTotal[dataTotal$storeId==curStore,]
    #curData <- dataTotalDT[storeId==curStore,]
    curLM <- lm(volumeSales~price+promo,data = curData)
    coef[curStore] <- coefficients(curLM)[3]  
  }
})
summary(coef)
hist(coef)


# Linear regression per store (for loop, subsetting using data.table "dataTotalDT")
t1b <- system.time({
  coef <- c()
  for (curStore in storesList){
    #curData <- dataTotal[dataTotal$storeId==curStore,]
    curData <- dataTotalDT[storeId==curStore,]
    curLM <- lm(volumeSales~price+promo,data = curData)
    coef[curStore] <- coefficients(curLM)[3]  
  }
})



# Linear regression per store (loop inside data.table "dataTotalDT" using function "lmfun")
lmfun <- function(curData){
  curLM <- lm(volumeSales~price+promo,data = curData)
  return(coefficients(curLM)[3])
}

t2 <- system.time( coefs <- dataTotalDT[,list( promoCoef=lmfun(.SD) ),by=storeId] )




# Linear regression per store (loop inside data.table "dataTotalDT" using function "lmfun", parallelized)
library(doParallel)
numCores <- 2
cl <- makeCluster(numCores)
registerDoParallel(cl)
numGroups <- numCores
dataTotalDT[,groupId:=rep(1:numGroups,each=numRows/numGroups)]
setkey(dataTotalDT,groupId)


lmfun2 <- function(dataTotalDT,curGroupId){
  library(data.table)
  curData <- dataTotalDT[groupId==curGroupId,]
  coefs <- curData[,list( promoCoef=lmfun(.SD) ),by=storeId]
  return(coefs)
}


t3<-system.time(coefList <- foreach(curGroupId=1:numGroups) %dopar% { lmfun2(dataTotalDT,curGroupId) })
outTotal <- rbindlist(coefList)
stopCluster(cl)
cat("For:", t1[3],"For (DT):", t1b[3], " DT:", t2[3], " DT parallel:",t3[3],"\n")






# Same plot using dygraphs package
library(dygraphs)
library(knitr)
library(xts)


cols <- c("blue", "green", "red")

plotTitle <- paste0("Linear Model of store ",curStore)

dataSeries <- data.frame(Week=as.Date(curData$weekId*7+13000),ActualSales=curData$volumeSales,
                        PredictedSales=fitted(curLM))
dataSeriesTS <- xts(dataSeries[,-1],order.by = dataSeries[,1])
dygraph(dataSeriesTS, main = plotTitle)  %>% 
  dyAxis("y", label = "Sales")        %>%
  dyRangeSelector()                    %>%
  dyAxis("x", drawGrid = FALSE)        %>%
  dyOptions(colors = cols,drawPoints = TRUE, pointSize = 2) %>%
  dyLegend(width = 600)

# To install rCharts
# require(devtools)
# install_github('ramnathv/rCharts')
# library(rCharts)

library(rCharts)
dataToPlot <- transform(dataSeries, date = as.character(Week))

m1 <- mPlot(x = "date", y = c("ActualSales", "PredictedSales"), type = "Line", data = dataToPlot)
m1$set(pointSize = 1, lineWidth = 1)
m1$show()
m1$save("mplot1.html",cdn=T)
