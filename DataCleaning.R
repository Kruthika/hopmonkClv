rm(list=ls(all=TRUE))

#DIY Set directory and read the data 
setwd("/Users/kruthikapotlapally/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv")

#Loading data
hopmonkClv <- read.csv("hopmonkClv.csv")
head(hopmonkClv)
dim(hopmonkClv)
str(hopmonkClv)
summary(hopmonkClv)

#Dropping the columns that are not required
requiredData <- subset(hopmonkClv, select = -c(X,CONTACT_WID,NominationDate,OveralllastTransaction))
str(requiredData)

#Getting numeric data
NumericData <- subset(requiredData, select = -c(Country,GameStrength,FavoriteGameBin,FavoriteGameBin7,FavoriteGameBin30,
                                                FavoriteGameBin90,FavoriteGameBin180,FavoriteGameBin360,
                                                FavouriteSource,FavouriteSource7,FavouriteSource30,FavouriteSource90,
                                                FavouriteSource180,FavouriteSource360,FavouriteChannel,FavouriteChannel7,
                                                FavouriteChannel30,FavouriteChannel90,FavouriteChannel180,FavouriteChannel360))
CategoricalData<- subset(requiredData, select = c(Country,GameStrength,FavoriteGameBin,FavoriteGameBin7,FavoriteGameBin30,
                                                   FavoriteGameBin90,FavoriteGameBin180,FavoriteGameBin360,
                                                   FavouriteSource,FavouriteSource7,FavouriteSource30,FavouriteSource90,
                                                   FavouriteSource180,FavouriteSource360,FavouriteChannel,FavouriteChannel7,
                                                   FavouriteChannel30,FavouriteChannel90,FavouriteChannel180,FavouriteChannel360))


#install.packages("outliers")
library(outliers)
###Splitting data into 2 sets to remove outliers
LowMediumData = NumericData[NumericData$TotalRevenueGenerated < 80 & NumericData$TotalRevenueGenerated > 0,]
HighRevenueGenerating = NumericData[NumericData$TotalRevenueGenerated > 80 & NumericData$TotalRevenueGenerated < 245,]
head(HighRevenueGenerating)

###For HighRevenueGenrating Data
count = 0
for (i in names(HighRevenueGenerating)) {
  percentOfZero = nrow(HighRevenueGenerating[HighRevenueGenerating[[i]] == 0,])*100/nrow(HighRevenueGenerating)
  if(percentOfZero < 20) {
    print(paste(i," has lesser than ", percentOfZero, " zeros. More information"))
    count = count + 1
  }
}
print(paste("Number of columns with higher information - ", count))

meanHigh <- mean(HighRevenueGenerating$TotalRevenueGenerated)
meanHigh
medianHigh <- median(HighRevenueGenerating$TotalRevenueGenerated)
medianHigh
hist(HighRevenueGenerating$TotalRevenueGenerated)
boxplot(HighRevenueGenerating$TotalRevenueGenerated, horizontal=TRUE)
summary(HighRevenueGenerating)

#Standardize the data
library(vegan)
range <- decostand((HighRevenueGenerating), "standardize")


#Getting Basic stats
count = 0;
for (i in names(range)) {
  mean <- mean(range[[ i ]] )
  median <- median(range[[ i ]] )
  std_dev <- sd(range[[ i ]]) 
  # diff = abs(mean - median)
  # print(diff)
  z = scores(range[[ i ]])
  
   if (z > 3 | z < - 3 ) {
    print(i)
    print(paste("Mean - ", mean))
    print(paste("Median - ", median))
    print(paste("Standard deviation - ", std_dev))
    count = count + 1
  }
  
  library(e1071)
  skewness <- skewness(range[[ i ]])
  kurtosis <- kurtosis(range[[ i ]]) 
  
  if(abs(kurtosis) < 1.96 | abs(skewness) < 1.96) {
    print(i)
    print(paste("Skewness - ", skewness))
    print(paste("Kurtosis - ", kurtosis))
    hist(range[[ i ]],xlab= i, main=i)
    boxplot(range[[ i ]], horizontal=TRUE,xlab= i, main=i)
    
  }
}

#Finding out outliers using quartiles and interquartiles
out <- outlier(NumericData, logical=TRUE)
# Actual outlier values
outliers = which(out[,], TRUE)

lowMediumMedian <- median(LowMediumData$TotalRevenueGenerated)
lowMediumMedian

#Further splitting data 
LowData <- LowMediumData[LowMediumData$TotalRevenueGenerated < 24,]
meanLow = mean(LowData$TotalRevenueGenerated)
meanLow
medianLow = median(LowData$TotalRevenueGenerated)
medianLow
hist(LowData$TotalRevenueGenerated)
boxplot(LowData$TotalRevenueGenerated, horizontal=TRUE)

#Check for all the columns that have more than 22% of 0s
count = 0
for (i in names(LowData)) {
  percentOfZero = nrow(LowData[LowData[[i]] == 0,])*100/nrow(LowData)
  if(percentOfZero < 22) {
    print(paste(i," has lesser than ", percentOfZero, " zeros. More information"))
    count = count + 1
  }
}
print(paste("Number of columns with higher information - ", count))



MediumData <- LowMediumData[LowMediumData$TotalRevenueGenerated > 24,]
meanMedium <- mean(MediumData$TotalRevenueGenerated)
meanMedium
medianMedium <- median(MediumData$TotalRevenueGenerated)
medianMedium
hist(MediumData$TotalRevenueGenerated)
boxplot(MediumData$TotalRevenueGenerated, horizontal=TRUE)

#Check for all the columns that have more than 22% of 0s
count = 0
for (i in names(MediumData)) {
  percentOfZero = nrow(MediumData[MediumData[[i]] == 0,])*100/nrow(MediumData)
  if(percentOfZero < 22) {
    print(paste(i," has lesser than ", percentOfZero, " zeros. More information"))
    count = count + 1
  }
}
print(paste("Number of columns with higher information - ", count))









