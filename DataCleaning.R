rm(list=ls(all=TRUE))

#DIY Set directory and read the data 
setwd("/Users/kruthikapotlapally/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv")

#Loading data
hopmonkClv <- read.csv("hopmonkClv.csv")
head(hopmonkClv)
dim(hopmonkClv)
str(hopmonkClv)
summary(hopmonkClv)

dataSelection <-  subset(hopmonkClv, select = c(Country,GameStrength,FavouriteSource,MinChildAge,
                                               MaxChildAge,NumHouseChildren,NumMaleChildrenHousehold,
                                               NumFemaleChildrenHousehold,NumGamesBought,
                                               FrequencyApp,RecencyApp,UNITS,TotalRevenueGenerated,FreqGamePlay,
                                               TotalTimeGamePlay,NumGamesPlayed,Recencydown,minRecencyCum,
                                               maxRecencyCum,minRecencyCum30))

summary(dataSelection) 




LowData = dataSelection[dataSelection$TotalRevenueGenerated > 0 & dataSelection$TotalRevenueGenerated < 24,]
library(Mass)

count = 0
for (i in names(LowData)) {
  percentOfZero = nrow(LowData[LowData[[i]] == 0,])*100/nrow(LowData)
  if(percentOfZero < 22) {
    print(paste(i," has lesser than ", percentOfZero, " zeros. More information"))
    count = count + 1
  }
}
print(paste("Number of columns with higher information - ", count))


numeric_Variables = subset(LowData, select = c(MinChildAge,MaxChildAge,NumHouseChildren,NumGamesBought,
                                                  FrequencyApp,RecencyApp,UNITS,
                                                  Recencydown,minRecencyCum,
                                                  maxRecencyCum,minRecencyCum30)) 
cat_variables = subset(LowData, select = c(Country,GameStrength,FavouriteSource))
numeric <- decostand(na.omit(numeric_Variables), "standardize")
target_variable = subset(LowData,select="TotalRevenueGenerated")
catDummies <- model.matrix(target_variable$TotalRevenueGenerated ~ cat_variables$FavouriteSource + cat_variables$GameStrength
                           +cat_variables$Country)[,-1]

#LinReg<-lm(TotalRevenueGenerated~.,data = LowData)
#Step <- stepAIC(LinReg, direction="both")

data =  as.matrix(data.frame(numeric, catDummies))
#Split the data into train and test data sets
rows=seq(1,nrow(data),1)
set.seed(123)
trainRows=sample(rows,(70*nrow(data))/100)


train = data[trainRows,] 
test = data[-trainRows,]

#Target Variable
y=target_variable$TotalRevenueGenerated[trainRows]
ytest = target_variable$TotalRevenueGenerated[-trainRows]

library(glmnet)
######################################################
# fit model
fit1=glmnet(train,y,alpha=1)  #LASSO
plot(fit1,xvar="lambda",label=TRUE)

fit2=glmnet(train,y,alpha=0)  #Ridge
plot(fit2,xvar="lambda",label=TRUE)

#######################################################
#cv.glmnet will help you choose lambda
cv <- cv.glmnet(train,y)  #By default alpha=1

#lambda.min - value of lambda that gives minimum cvm - mean cross-validated error

#######################################################
# LASSO Regression  using glmnet - L1 norm
fit1=glmnet(train,y,lambda=cv$lambda.min,alpha=1)
predict(fit1,train)
library(DMwR)
LASSOtrain = regr.eval(y, predict(fit1,train))
LASSOtest = regr.eval(ytest, predict(fit1,test))
LASSOtrain
LASSOtest


#Model Selection
coef(fit1)
cv.lasso=cv.glmnet(train,y)
plot(cv.lasso)
coef(cv.lasso)

#############################################################################
# Ridge Regression  using glmnet  - L2 norm
library(glmnet)
# fit model
fit2=glmnet(train,y,lambda=cv$lambda.min,alpha=0)
predict(fit2,train)

RIDGEtrain = regr.eval(y, predict(fit2,train))
RIDGEtest = regr.eval(ytest, predict(fit2,test))
RIDGEtrain
RIDGEtest
#Model Selection
coef(fit2) 
cv.ridge=cv.glmnet(train,y,alpha=0)
plot(cv.ridge)
coef(cv.ridge)

#cvfit = cv.glmnet(train,y,alpha=0, type.measure = "mae")
################################################################
# Elastic regression
fit3=glmnet(train,y,lambda=cv$lambda.min,alpha=0.5)
# summarize the fit
summary(fit3)
predict(fit3,train)

Elastictrain = regr.eval(y, predict(fit3,train))
Elastictest = regr.eval(ytest, predict(fit3,test))
Elastictrain
Elastictest

# make predictions
predictions <- predict(fit3, train, type="link")
# summarize accuracy
rmse <- mean((y - predictions)^2)
print(rmse)

#################################################

LASSOtrain
LASSOtest
RIDGEtrain
RIDGEtest
Elastictrain
Elastictest

finalerrors <- data.frame(rbind(
  LASSOtrain,LASSOtest,
  RIDGEtrain,RIDGEtest,
  Elastictrain,Elastictest))
finalerrors


##################################################

HighRevenueGenerating = dataSelection[dataSelection$TotalRevenueGenerated > 80 & dataSelection$TotalRevenueGenerated < 245,]

count = 0
for (i in names(HighRevenueGenerating)) {
  percentOfZero = nrow(HighRevenueGenerating[HighRevenueGenerating[[i]] == 0,])*100/nrow(HighRevenueGenerating)
  if(percentOfZero < 22) {
    print(paste(i," has lesser than ", percentOfZero, " zeros. More information"))
    count = count + 1
  }
}
print(paste("Number of columns with higher information - ", count))


numeric_Variables = subset(HighRevenueGenerating, select = c(MinChildAge,MaxChildAge,NumHouseChildren,NumGamesBought,
                                               FrequencyApp,RecencyApp,UNITS,
                                               Recencydown,minRecencyCum, FreqGamePlay,TotalTimeGamePlay,NumGamesPlayed,
                                               maxRecencyCum)) 
cat_variables = subset(HighRevenueGenerating, select = c(Country,GameStrength,FavouriteSource))
numeric <- decostand(na.omit(numeric_Variables), "standardize")
target_variable = subset(HighRevenueGenerating,select="TotalRevenueGenerated")
catDummies <- model.matrix(target_variable$TotalRevenueGenerated ~ cat_variables$FavouriteSource + cat_variables$GameStrength
                           +cat_variables$Country)[,-1]

#LinReg<-lm(TotalRevenueGenerated~.,data = HighRevenueGenerating)
#Step <- stepAIC(LinReg, direction="both")

data =  as.matrix(data.frame(numeric, catDummies))
#Split the data into train and test data sets
rows=seq(1,nrow(data),1)
set.seed(123)
trainRows=sample(rows,(70*nrow(data))/100)


train = data[trainRows,] 
test = data[-trainRows,]

#Target Variable
y=target_variable$TotalRevenueGenerated[trainRows]
ytest = target_variable$TotalRevenueGenerated[-trainRows]

library(glmnet)
######################################################
# fit model
fit1=glmnet(train,y,alpha=1)  #LASSO
plot(fit1,xvar="lambda",label=TRUE)

fit2=glmnet(train,y,alpha=0)  #Ridge
plot(fit2,xvar="lambda",label=TRUE)

#######################################################
#cv.glmnet will help you choose lambda
cv <- cv.glmnet(train,y)  #By default alpha=1

#lambda.min - value of lambda that gives minimum cvm - mean cross-validated error

#######################################################
# LASSO Regression  using glmnet - L1 norm
fit1=glmnet(train,y,lambda=cv$lambda.min,alpha=1)
predict(fit1,train)
library(DMwR)
LASSOtrain = regr.eval(y, predict(fit1,train))
LASSOtest = regr.eval(ytest, predict(fit1,test))
LASSOtrain
LASSOtest


#Model Selection
coef(fit1)
cv.lasso=cv.glmnet(train,y)
plot(cv.lasso)
coef(cv.lasso)

#############################################################################
# Ridge Regression  using glmnet  - L2 norm
library(glmnet)
# fit model
fit2=glmnet(train,y,lambda=cv$lambda.min,alpha=0)
predict(fit2,train)

RIDGEtrain = regr.eval(y, predict(fit2,train))
RIDGEtest = regr.eval(ytest, predict(fit2,test))
RIDGEtrain
RIDGEtest
#Model Selection
coef(fit2) 
cv.ridge=cv.glmnet(train,y,alpha=0)
plot(cv.ridge)
coef(cv.ridge)

#cvfit = cv.glmnet(train,y,alpha=0, type.measure = "mae")
################################################################
# Elastic regression
fit3=glmnet(train,y,lambda=cv$lambda.min,alpha=0.5)
# summarize the fit
summary(fit3)
predict(fit3,train)

Elastictrain = regr.eval(y, predict(fit3,train))
Elastictest = regr.eval(ytest, predict(fit3,test))
Elastictrain
Elastictest

# make predictions
predictions <- predict(fit3, train, type="link")
# summarize accuracy
rmse <- mean((y - predictions)^2)
print(rmse)

#################################################

LASSOtrain
LASSOtest
RIDGEtrain
RIDGEtest
Elastictrain
Elastictest

finalerrors <- data.frame(rbind(
  LASSOtrain,LASSOtest,
  RIDGEtrain,RIDGEtest,
  Elastictrain,Elastictest))
finalerrors

#####################################################
MediumData <- dataSelection[dataSelection$TotalRevenueGenerated > 24 & dataSelection$TotalRevenueGenerated < 80,]

count = 0
for (i in names(MediumData)) {
  percentOfZero = nrow(MediumData[LowData[[i]] == 0,])*100/nrow(MediumData)
  if(percentOfZero < 22) {
    print(paste(i," has lesser than ", percentOfZero, " zeros. More information"))
    count = count + 1
  }
}
print(paste("Number of columns with higher information - ", count))


numeric_Variables = subset(MediumData, select = c(MaxChildAge,NumHouseChildren,NumGamesBought,NumMaleChildrenHousehold,
                                               FrequencyApp,RecencyApp,UNITS,TotalTimeGamePlay,
                                               Recencydown,minRecencyCum,NumGamesPlayed,FreqGamePlay,
                                               maxRecencyCum,minRecencyCum30)) 
cat_variables = subset(MediumData, select = c(Country,GameStrength,FavouriteSource))
numeric <- decostand(na.omit(numeric_Variables), "standardize")
target_variable = subset(MediumData,select="TotalRevenueGenerated")
catDummies <- model.matrix(target_variable$TotalRevenueGenerated ~ cat_variables$FavouriteSource + cat_variables$GameStrength
                           +cat_variables$Country)[,-1]
#LinReg<-lm(TotalRevenueGenerated~.,data = MediumData)
#Step <- stepAIC(LinReg, direction="both")

data =  as.matrix(data.frame(numeric, catDummies))
#Split the data into train and test data sets
rows=seq(1,nrow(data),1)
set.seed(123)
trainRows=sample(rows,(70*nrow(data))/100)


train = data[trainRows,] 
test = data[-trainRows,]

#Target Variable
y=target_variable$TotalRevenueGenerated[trainRows]
ytest = target_variable$TotalRevenueGenerated[-trainRows]

library(glmnet)
######################################################
# fit model
fit1=glmnet(train,y,alpha=1)  #LASSO
plot(fit1,xvar="lambda",label=TRUE)

fit2=glmnet(train,y,alpha=0)  #Ridge
plot(fit2,xvar="lambda",label=TRUE)

#######################################################
#cv.glmnet will help you choose lambda
cv <- cv.glmnet(train,y)  #By default alpha=1

#lambda.min - value of lambda that gives minimum cvm - mean cross-validated error

#######################################################
# LASSO Regression  using glmnet - L1 norm
fit1=glmnet(train,y,lambda=cv$lambda.min,alpha=1)
predict(fit1,train)
library(DMwR)
LASSOtrain = regr.eval(y, predict(fit1,train))
LASSOtest = regr.eval(ytest, predict(fit1,test))
LASSOtrain
LASSOtest


#Model Selection
coef(fit1)
cv.lasso=cv.glmnet(train,y)
plot(cv.lasso)
coef(cv.lasso)

#############################################################################
# Ridge Regression  using glmnet  - L2 norm
library(glmnet)
# fit model
fit2=glmnet(train,y,lambda=cv$lambda.min,alpha=0)
predict(fit2,train)

RIDGEtrain = regr.eval(y, predict(fit2,train))
RIDGEtest = regr.eval(ytest, predict(fit2,test))
RIDGEtrain
RIDGEtest
#Model Selection
coef(fit2) 
cv.ridge=cv.glmnet(train,y,alpha=0)
plot(cv.ridge)
coef(cv.ridge)

#cvfit = cv.glmnet(train,y,alpha=0, type.measure = "mae")
################################################################
# Elastic regression
fit3=glmnet(train,y,lambda=cv$lambda.min,alpha=0.5)
# summarize the fit
summary(fit3)
predict(fit3,train)

Elastictrain = regr.eval(y, predict(fit3,train))
Elastictest = regr.eval(ytest, predict(fit3,test))
Elastictrain
Elastictest

# make predictions
predictions <- predict(fit3, train, type="link")
# summarize accuracy
rmse <- mean((y - predictions)^2)
print(rmse)

#################################################

LASSOtrain
LASSOtest
RIDGEtrain
RIDGEtest
Elastictrain
Elastictest

finalerrors <- data.frame(rbind(
  LASSOtrain,LASSOtest,
  RIDGEtrain,RIDGEtest,
  Elastictrain,Elastictest))
finalerrors



