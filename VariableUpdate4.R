rm(list=ls(all=TRUE))

#DIY Set directory and read the data 
setwd("/Users/kruthikapotlapally/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv")

###There were NAs in most data sets and I could not perform any operations on existing variables.

#reading and processing ownr.txt
data<-read.delim("~/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv/data/stg_bgdt_cust_ownr.txt", header =T,sep="\t")

names(data)

require(lubridate)
data$X_NOMIN_DT<-as.Date(mdy_hm(data$X_NOMIN_DT))

data<-data[( data$X_NOMIN_DT > "2012-12-24" & data$X_NOMIN_DT < "2013-01-27") & (data$X_EDW_INTEGRATION_ID == "kutti" | data$X_EDW_INTEGRATION_ID == "PAD2"),]
library(sqldf)

data$X_NOMIN_DT <- as.character(data$X_NOMIN_DT)
sqldf("SELECT 
      (SELECT count(*) FROM data) as NUM_ROWS,
      (SELECT count(DISTINCT CONTACT_WID) FROM data) as CONTACT_WID,
      (SELECT count(DISTINCT X_NOMIN_DT) FROM data) as X_NOMIN_DT,
      (SELECT count(DISTINCT DEVICE_SRL_NUM) FROM data) as DEVICE_SRL_NUM,
      (SELECT count(DISTINCT ASSET_WID) FROM data) as ASSET_WID,
      (SELECT count(DISTINCT X_EDW_INTEGRATION_ID) FROM data) as X_EDW_INTEGRATION_ID,
      (SELECT count(DISTINCT X_CNTRY) FROM data) as X_CNTRY,
      (SELECT count(DISTINCT X_TYPE) FROM data) as X_TYPE")

custOwner <- sqldf("SELECT CONTACT_WID, min(X_NOMIN_DT) as NominationDate , min(X_CNTRY) as Country from data group by CONTACT_WID")

###########################################################################################################

data3<- read.delim("~/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv/data/stg_bgdt_cust_chld.txt", header=T, sep="\t")

colnames(data3)[which(names(data3) == "X")] <- "CHILD_CREATION_DATEE"
colnames(data3)[which(names(data3) == "SEX_MF_CODE")] <- "CHILD_AGE"
colnames(data3)[which(names(data3) == "X_GRADE")] <- "SEX_MF_CODE"
colnames(data3)[which(names(data3) == "CHILD_CREATION_DATE")] <- "X_GRADE"
colnames(data3)[which(names(data3) == "CHILD_CREATION_DATEE")] <- "CHILD_CREATION_DATE"
#data3<-na.omit(data3)
#sum(is.na(data3))

data3$CHILD_BIRTH_DATE<-as.Date(mdy_hm(data3$CHILD_BIRTH_DATE ))

names(data3)
sqldf("SELECT 
      (SELECT count(*) FROM data3) as NUM_ROWS,
      (SELECT count(DISTINCT CONTACT_WID) FROM data3) as CONTACT_WID,
      (SELECT count(DISTINCT CHILD_ROW_WID) FROM data3) as CHILD_ROW_WID,
      (SELECT count(DISTINCT PR_HOUSEHOLD_WID) FROM data3) as PR_HOUSEHOLD_WID,
      (SELECT count(DISTINCT X_CRM_CUST_KEY) FROM data3) as X_CRM_CUST_KEY,
      (SELECT count(DISTINCT CRM_CHLD_KEY) FROM data3) as CRM_CHLD_KEY,
      (SELECT count(DISTINCT CHILD_FIRST_NAME) FROM data3) as CHILD_FIRST_NAME,
      (SELECT count(DISTINCT CHILD_BIRTH_DATE) FROM data3) as CHILD_BIRTH_DATE,
      (SELECT count(DISTINCT CHILD_AGE) FROM data3) as CHILD_AGE,
      (SELECT count(DISTINCT SEX_MF_CODE) FROM data3) as SEX_MF_CODE,
      (SELECT count(DISTINCT X_GRADE) FROM data3) as X_GRADE,
      (SELECT count(DISTINCT CHILD_CREATION_DATE) FROM data3) as CHILD_CREATION_DATE")

####NA values in PR_HOUSEHOLD_WID
sqldf("SELECT count(DISTINCT CONTACT_WID), PR_HOUSEHOLD_WID  FROM data3 GROUP BY PR_HOUSEHOLD_WID LIMIT 1")

custChild <- sqldf("SELECT CONTACT_WID, min(CHILD_AGE) as MinChildAge, max(CHILD_AGE) as MaxChildAge,
                    (max(CHILD_AGE)-min(CHILD_AGE)) as ChildAgeRange FROM data3 GROUP BY CONTACT_WID")


##########################################################################################################
#reading and processing cust_purc_app.txt
data6<- read.delim("~/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv/data/stg_bgdt_cust_purc_app.txt", header=T, sep="\t")
data6$TRANSACTION_DATE<-as.Date(mdy_hm(data6$TRANSACTION_DATE ))
data6 <- data6[(data6$TRANSACTION_DATE > "2011-12-25") ,]
data6$TRANSACTION_DATE = as.character(data6$TRANSACTION_DATE)
class(data6$TRANSACTION_DATE)

sqldf("SELECT 
      (SELECT count(*) FROM data6) as NUM_ROWS,
      (SELECT count(DISTINCT CONTACT_WID) FROM data6) as CONTACT_WID,
      (SELECT count(DISTINCT X_CRM_CUST_KEY) FROM data6) as X_CRM_CUST_KEY,
      (SELECT count(DISTINCT CHANNEL_DESCRIPTION) FROM data6) as CHANNEL_DESCRIPTION,
      (SELECT count(DISTINCT TRANSACTION_DATE) FROM data6) as TRANSACTION_DATE,
      (SELECT count(DISTINCT ITEM_NUMBER) FROM data6) as ITEM_NUMBER,
      (SELECT count(DISTINCT ITEM_DESCRIPTION) FROM data6) as ITEM_DESCRIPTION,
      (SELECT count(DISTINCT TRANSACTION_STORE_FRONT) FROM data6) as TRANSACTION_STORE_FRONT,
      (SELECT count(DISTINCT SOURCE_OF_PURCHASE) FROM data6) as SOURCE_OF_PURCHASE,
      (SELECT count(DISTINCT LIC_CD) FROM data6) as LIC_CD,
      (SELECT count(DISTINCT INSTALLED_PLATFORM) FROM data6) as INSTALLED_PLATFORM,
      (SELECT count(DISTINCT UNITS) FROM data6) as UNITS,
      (SELECT count(DISTINCT AMOUNT_USD) FROM data6) as AMOUNT_USD")

custPurcApp <- sqldf("select CONTACT_WID, MAX(TRANSACTION_DATE) AS TRANSACTION_DATE, 
      SUM(UNITS) AS UNITS,
      SUM(CASE 
      WHEN TRANSACTION_DATE < '2012-01-02'  THEN UNITS
      ELSE 0
      END) AS UNIT7,
      SUM(CASE 
      WHEN TRANSACTION_DATE < '2012-01-26'  THEN UNITS
      ELSE 0
      END) AS UNIT30,
      SUM(CASE 
      WHEN TRANSACTION_DATE < '2012-03-26' THEN UNITS
      ELSE 0
      END) AS UNIT90,
      SUM(CASE 
      WHEN TRANSACTION_DATE < '2012-06-23' THEN UNITS
      ELSE 0
      END) AS UNIT180,
      SUM(CASE 
      WHEN TRANSACTION_DATE < '2012-12-26'  THEN UNITS
      ELSE 0
      END) AS UNIT360
      FROM data6
      group by CONTACT_WID")

FrequencyApp <- sqldf("select CONTACT_WID, MAX(TRANSACTION_DATE) AS TRANSACTION_DATE, 
                     SUM(CASE
                     WHEN TRANSACTION_DATE NOTNULL THEN 1
                     ELSE 0
                     END) AS FrequencyApp,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-01-02' AND TRANSACTION_DATE NOTNULL) THEN 1
                     ELSE 0
                     END) AS FrequencyApp7,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-01-26' AND TRANSACTION_DATE NOTNULL)  THEN 1
                     ELSE 0
                     END) AS FrequencyApp30,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-03-26' AND TRANSACTION_DATE NOTNULL) THEN 1
                     ELSE 0
                     END) AS FrequencyApp90,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-06-23' AND TRANSACTION_DATE NOTNULL) THEN 1
                     ELSE 0
                     END) AS FrequencyApp180,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-12-26' AND TRANSACTION_DATE NOTNULL)  THEN 1
                     ELSE 0
                     END) AS FrequencyApp360
                     FROM data6
                     group by CONTACT_WID")



##########################################################################################################
#reading and processing cust_purc_lf.txt
data7<- read.delim("~/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv/data/stg_bgdt_cust_purc_lf.txt", header=T, sep="\t")
data7$TRANSACTION_DT<-as.Date(dmy_hm(data7$TRANSACTION_DT ))
data7 <- data7[(data7$TRANSACTION_DT>"2011-12-25") ,]
data7$TRANSACTION_DT = as.character(data7$TRANSACTION_DT)
#data7<-na.omit(data7)
#sum(is.na(data7))
names(data7)

sqldf("SELECT 
      (SELECT count(*) FROM data7) as NUM_ROWS,
      (SELECT count(DISTINCT CONTACT_WID) FROM data7) as CONTACT_WID,
      (SELECT count(DISTINCT X_CRM_CUST_KEY) FROM data7) as X_CRM_CUST_KEY,
      (SELECT count(DISTINCT ITEM) FROM data7) as ITEM,
      (SELECT count(DISTINCT ITEM_DESCR) FROM data7) as ITEM_DESCR,
      (SELECT count(DISTINCT TRANSACTION_DT) FROM data7) as TRANSACTION_DT,
      (SELECT count(DISTINCT UNITS) FROM data7) as UNITS,
      (SELECT count(DISTINCT AMOUNT) FROM data7) as AMOUNT")

custPurcLf <- sqldf("select CONTACT_WID, max(TRANSACTION_DT) AS OveralllastTransaction,
                 SUM(UNITS) AS UNITS,
                 SUM(CASE 
                 WHEN TRANSACTION_DT < '2012-01-02'  THEN UNITS
                 ELSE 0
                 END) AS UNIT7,
                 SUM(CASE 
                 WHEN TRANSACTION_DT < '2012-01-26'  THEN UNITS
                 ELSE 0
                 END) AS UNIT30,
                 SUM(CASE 
                 WHEN TRANSACTION_DT < '2012-03-26' THEN UNITS
                 ELSE 0
                 END) AS UNIT90,
                 SUM(CASE 
                 WHEN TRANSACTION_DT < '2012-06-23' THEN UNITS
                 ELSE 0
                 END) AS UNIT180,
                 SUM(CASE 
                 WHEN TRANSACTION_DT < '2012-12-26'  THEN UNITS
                 ELSE 0
                 END) AS UNIT360
                 FROM data7
                 group by CONTACT_WID")


FrequencyLF <- sqldf("select CONTACT_WID, MAX(TRANSACTION_DT) AS TRANSACTION_DT, 
                     SUM(CASE
                      WHEN TRANSACTION_DT NOTNULL THEN 1
                      ELSE 0
                      END) AS FrequencyLF,
                      SUM(CASE 
                      WHEN (TRANSACTION_DT < '2012-01-02' AND TRANSACTION_DT NOTNULL) THEN 1
                      ELSE 0
                      END) AS FrequencyLF7,
                      SUM(CASE 
                      WHEN (TRANSACTION_DT < '2012-01-26' AND TRANSACTION_DT NOTNULL)  THEN 1
                      ELSE 0
                      END) AS FrequencyLF30,
                      SUM(CASE 
                      WHEN (TRANSACTION_DT < '2012-03-26' AND TRANSACTION_DT NOTNULL) THEN 1
                      ELSE 0
                      END) AS FrequencyLF90,
                      SUM(CASE 
                      WHEN (TRANSACTION_DT < '2012-06-23' AND TRANSACTION_DT NOTNULL) THEN 1
                      ELSE 0
                      END) AS FrequencyLF180,
                      SUM(CASE 
                      WHEN (TRANSACTION_DT < '2012-12-26' AND TRANSACTION_DT NOTNULL)  THEN 1
                      ELSE 0
                      END) AS FrequencyLF360
                      FROM data7
                      group by CONTACT_WID")


##########################################################################################################

#Merging custPurcApp and custPurcLf tables

units <- sqldf("SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID, 
              COALESCE(a.TRANSACTION_DATE, b.OveralllastTransaction) AS TRANSACTION_DATE, 
              (coalesce(a.UNITS,0) + coalesce(b.Units,0)) as UNITS, 
              (coalesce(a.UNIT7,0) + coalesce(b.UNIT7,0)) AS UNIT7,
              (coalesce(a.UNIT30,0) + coalesce(b.UNIT30,0)) AS UNIT30, 
              (coalesce(a.UNIT90,0) + coalesce(b.UNIT90,0)) AS UNIT90, 
              (coalesce(a.UNIT180,0) + coalesce(b.UNIT180,0)) AS UNIT180, 
              (coalesce(a.UNIT360,0) + coalesce(b.UNIT360,0)) AS UNIT360
              FROM custPurcApp a LEFT JOIN custPurcLf b ON a.CONTACT_WID = b.CONTACT_WID
              UNION ALL
              SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID, 
              COALESCE(b.OveralllastTransaction, a.TRANSACTION_DATE ) AS TRANSACTION_DATE, 
              (coalesce(a.UNITS,0) + coalesce(b.Units,0)) as UNITS, 
              (coalesce(a.UNIT7,0) + coalesce(b.UNIT7,0)) AS UNIT7,
              (coalesce(a.UNIT30,0) + coalesce(b.UNIT30,0)) AS UNIT30, 
              (coalesce(a.UNIT90,0) + coalesce(b.UNIT90,0)) AS UNIT90, 
              (coalesce(a.UNIT180,0) + coalesce(b.UNIT180,0)) AS UNIT180, 
              (coalesce(a.UNIT360,0) + coalesce(b.UNIT360,0)) AS UNIT360
              FROM custPurcLf b LEFT JOIN custPurcApp a ON a.CONTACT_WID = b.CONTACT_WID
              WHERE b.CONTACT_WID IS NULL")


#########################################################################################################
#Tenure
merge(x = custOwner, y = custPurcLf, by = "CONTACT_WID", all = TRUE)

custOwner$NominationDate<-as.Date(mdy_hm(custOwner$NominationDate))
class(custOwner$NominationDate)
custPurcLf$OveralllastTransaction <- as.Date(mdy_hm(custPurcLf$OveralllastTransaction))
class(custPurcLf$OveralllastTransaction)

sqldf("SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID, 
              (COALESCE(a.NominationDate,0) - COALESCE(b.OveralllastTransaction,0)) AS TenureDays 
      FROM custOwner a LEFT JOIN custPurcLf b ON a.CONTACT_WID = b.CONTACT_WID
      UNION ALL
      SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID, 
      (COALESCE(b.OveralllastTransaction,0) - COALESCE(a.NominationDate,0) ) AS TenureDays
      FROM custPurcLf b LEFT JOIN custOwner a ON a.CONTACT_WID = b.CONTACT_WID
      WHERE b.CONTACT_WID IS NULL")
######################################################################################################
##reading and processing cust_gam_actv.txt

data4<- read.delim("~/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv/data/stg_bgdt_cust_gam_actv.csv", header=T, sep=",")

data4$TITLE_NOMIN_DT<-as.Date(dmy(data4$TITLE_NOMIN_DT ))
data4 <- data4[(data4$TITLE_NOMIN_DT > "2011-12-25"),]
names(data4)
data4$TITLE_NOMIN_DT <- as.character(data4$TITLE_NOMIN_DT)

custGamActv <- sqldf("select CONTACT_WID, max(TITLE_NOMIN_DT) AS OveralllastTransaction,
                    SUM(ATMP_CNT) AS FreqGamePlay,
                    SUM(CASE 
                    WHEN TITLE_NOMIN_DT < '2012-01-02'  THEN ATMP_CNT
                    ELSE 0
                    END) AS FreqGamePlay7,
                    SUM(CASE 
                    WHEN TITLE_NOMIN_DT < '2012-01-26'  THEN ATMP_CNT
                    ELSE 0
                    END) AS FreqGamePlay30,
                    SUM(CASE 
                    WHEN TITLE_NOMIN_DT < '2012-03-26' THEN ATMP_CNT
                    ELSE 0
                    END) AS FreqGamePlay90,
                    SUM(CASE 
                    WHEN TITLE_NOMIN_DT < '2012-06-23' THEN ATMP_CNT
                    ELSE 0
                    END) AS FreqGamePlay180,
                    SUM(CASE 
                    WHEN TITLE_NOMIN_DT < '2012-12-26'  THEN ATMP_CNT
                    ELSE 0
                    END) AS FreqGamePlay360
                    FROM data4
                    group by CONTACT_WID")

######################################################################################################
#reading and processing cust_app_dwnld.txt
data2t <- read.delim("stg_bgdt_cust_app_dwnld.txt", header=T, sep="\t")
#NOMIN_DT > "2011-12-25"
data2<- data2t

colnames(data2)[which(names(data2) == "NOMIN_DT")] <- "DEVICE_DESCRI"
colnames(data2)[which(names(data2) == "DEVICE_DESCR")] <- "NOMIN_DT"
colnames(data2)[which(names(data2) == "DEVICE_DESCRI")] <- "DEVICE_DESCR"
colnames(data2)[which(names(data2) == "ITEM_NUMBER")] <- "ITEM_NAMEE"
colnames(data2)[which(names(data2) == "ITEM_NAME")] <- "ITEM_NUMBER"
colnames(data2)[which(names(data2) == "ITEM_NAMEE")] <- "ITEM_NAME"

data2$NOMIN_DT<- as.Date(mdy_hm(data2$NOMIN_DT ))

data2 <- data2[(data2$NOMIN_DT > "2011-12-25"),]

data2<-na.omit(data2)
sum(is.na(data2))
#reading and processing cust_chld.txt

data3<- read.delim("stg_bgdt_cust_chld.txt", header=T, sep="\t")

colnames(data3)[which(names(data3) == "X")] <- "CHILD_CREATION_DATEE"
colnames(data3)[which(names(data3) == "SEX_MF_CODE")] <- "CHILD_AGE"
colnames(data3)[which(names(data3) == "X_GRADE")] <- "SEX_MF_CODE"
colnames(data3)[which(names(data3) == "CHILD_CREATION_DATE")] <- "X_GRADE"
colnames(data3)[which(names(data3) == "CHILD_CREATION_DATEE")] <- "CHILD_CREATION_DATE"
data3<-na.omit(data3)
sum(is.na(data3))
#library(tidyr)
#data3 %>% drop_na()

data3$CHILD_BIRTH_DATE<-as.Date(mdy_hm(data3$CHILD_BIRTH_DATE ))

#c<-apply(na.omit(data3),2,max); ## this will remove the NA rows from the data frame and then calculate the max values

#na.omit(data3)
#sum(is.na(data3))
#on hold
#data3 <- data2[(data3$CHILD_BIRTH_DATE > "2011-12-25"),]


##reading and processing cust_gam_play_cart.txt
data5<- read.delim("stg_bgdt_cust_gam_play_cart.txt", header=T, sep="\t")

data5$TITLE_NOMIN_DT<-as.Date(dmy_hm(data5$TITLE_NOMIN_DT ))

data5 <- data5[(data5$TITLE_NOMIN_DT > "2011-12-25" & data5$TITLE_NOMIN_DT < "2013-04-11"),]
data5<-na.omit(data5)
sum(is.na(data5))



#reading and processing cust_purc_lf.txt
data7<- read.delim("stg_bgdt_cust_purc_lf.txt", header=T, sep="\t")
data7$TRANSACTION_DT<-as.Date(dmy_hm(data7$TRANSACTION_DT ))
data7 <- data7[(data7$TRANSACTION_DT>"2011-12-25") ,]

data7<-na.omit(data7)
sum(is.na(data7))
names(data)

#########31st Jan 2018
############################ derived variables
NominationDate<- data.frame( tapply(data1$CONTACT_WID, data1$X_NOMIN_DT, min))
MaxChildAge<-data.frame( tapply(data3$CONTACT_WID, data3$CHILD_AGE, max))
MinChildAge<-data.frame( tapply(data3$CONTACT_WID, data3$CHILD_AGE, min))
TenureDays<-data.frame(OveralllastTransaction - data1$X_NOMIN_DT)
ChildAgeRange<- MaxChildAge - MinChildAge
Country<- data1$X_CNTRY
CONTACT_WID<- data1$CONTACT_WID
library(sqldf)
three_var<-sqldf("select CONTACT_WID,X_CNTRY from data1")
NominationDate<-data1[,1:2]
NominationDate <- NominationDate %>% group_by(CONTACT_WID) %>% summarise(min(X_NOMIN_DT))
three_var<-merge(x=three_var,y=NominationDate,all=T)
#three_var<- merge(three_var,MaxChildAge,all=T)
#three_var<-merge(three_var,MinChildAge,all=T)
four_var<-merge(x=three_var,y=ChildAgeRange,all=T)

two_var<-sqldf("select CONTACT_WID,CHILD_AGE from data3")
CHILD_AGE1<-data1[,1:8]
<- NominationDate %>% group_by(CONTACT_WID) %>% summarise(min(X_NOMIN_DT))
three_var<-merge(x=three_var,y=NominationDate,all=T)

######### UNITS##########
#######sum(UNITS) from STG_BGDT_CUST_PURC_LF.txt + sum(UNITS) from stg_bgdt_cust_purc_app.txt
M11<-merge(M,ChildAgeRange , by="CONTACT_WID", all = T)

?tapply
Units<- data7$UNITS + data6$UNITS
dim(data7$UNITS)

a<- data.frame(data1$CONTACT_WID, data1$X_CNTRY)

M<-merge(MaxChildAge,MinChildAge)
M1<- fulljoin(M,ChildAgeRange)
MD<- data.frame(,MaxChildAge,MinChildAge)
######NominationDate -----stg_bgdt_cust_ownr.txt----min(X_NOMIN_DT)
nomindate1<-min(data1$X_NOMIN_DT)
dim(a)
dim(NominationDate)
dim(MaxChildAge)
?fulljoin

############I met a friend who works on Data and figured out that using sql and this operation can save lots of time in extracting and performing operations on variables.
####### sqldf

#Units
tempUnits <- sqldf("SELECT A.CONTACT_WID,SUM(A.UNITS+B.UNITS) FROM data6 A LEFT JOIN
                   data7 B ON A.CONTACT_WID = B.CONTACT_WID GROUP BY A.CONTACT_WID")
Units <- sqldf("SELECT CONTACT_WID, SUM(UNITS) FROM data6 GROUP BY CONTACT_WID")

#Units7
tempUnits7 <- sqldf("SELECT A.CONTACT_WID,SUM(A.UNITS+B.UNITS) FROM data6 A LEFT JOIN
                    data7 B ON A.CONTACT_WID = B.CONTACT_WID GROUP BY A.CONTACT_WID")
Units7 <- sqldf("SELECT CONTACT_WID, SUM(UNITS) FROM data6 GROUP BY CONTACT_WID")

########### the below went waste###########

##stg_bgdt_cust_chld.txt ,
#max_child_age<-data.frame( tapply(data3$CONTACT_WID, data3$CHILD_AGE, max))
##Diffrence between the variables : MaxChildAge and MinChildAge
#ChildAgeRange<- max_child_age - min_child_age

######### UNITS##########
#######sum(UNITS) from STG_BGDT_CUST_PURC_LF.txt + sum(UNITS) from stg_bgdt_cust_purc_app.txt
Units<-data.frame(sum(data7$UNITS) + sum(data6$UNITS) )
U = (data7$UNITS) + (data6$UNITS)

str(data6$UNITS)
str(data7$UNITS)
####### Units7- sum(UNITS) from STG_BGDT_CUST_PURC_LF.txt + sum(UNITS) from stg_bgdt_cust_purc_app.txt
####TRANSACTION_DT < "2012-01-02"
Units7LF <- data7[(data7$TRANSACTION_DT < "2012-01-02") ,]
Units7app <- data6[(data6$TRANSACTION_DATE < "2012-01-02") ,]

########
Units7<-sum(Units7LF$UNITS) + sum(Units7app$UNITS)

######### Units30- sum(UNITS) from STG_BGDT_CUST_PURC_LF.txt + sum(UNITS) from stg_bgdt_cust_purc_app.txt
#####TRANSACTION_DT < "2012-01-26"

Units30LF<- data7[(data7$TRANSACTION_DT < "2012-01-26") ,]
Units30app<- data6[(data6$TRANSACTION_DATE < "2012-01-26") ,]

Units30<-sum(Units30LF$UNITS) + sum(Units30app$UNITS)
#U30<- Units30LF$UNITS + Units30app$UNITS

######### Units90- sum(UNITS) from STG_BGDT_CUST_PURC_LF.txt + sum(UNITS) from stg_bgdt_cust_purc_app.txt
#####TRANSACTION_DT < "2012-03-26"
Units90LF<- data7[(data7$TRANSACTION_DT < "2012-03-26") ,]
Units90app<- data6[(data6$TRANSACTION_DATE < "2012-03-26") ,]

Units90<-sum(Units90LF$UNITS) + sum(Units90app$UNITS)

######### Units180- sum(UNITS) from STG_BGDT_CUST_PURC_LF.txt + sum(UNITS) from stg_bgdt_cust_purc_app.txt
#####TRANSACTION_DT < "2012-06-23"
Units180LF<- data7[(data7$TRANSACTION_DT < "2012-06-23") ,]
Units180app<- data6[(data6$TRANSACTION_DATE < "2012-06-23") ,]

Units180<-sum(Units180LF$UNITS) + sum(Units180app$UNITS)

######### Units360- sum(UNITS) from STG_BGDT_CUST_PURC_LF.txt + sum(UNITS) from stg_bgdt_cust_purc_app.txt
#####TRANSACTION_DT < "2012-12-26"
Units360LF<- data7[(data7$TRANSACTION_DT < "2012-12-26") ,]
Units360app<- data6[(data6$TRANSACTION_DATE < "2012-12-26") ,]

Units360<-sum(Units360LF$UNITS) + sum(Units360app$UNITS)

########OveralllastTransaction----STG_BGDT_CUST_PURC_LF.txt---max(TRANSACTION_DT)

OveralllastTransaction <- max(data7$TRANSACTION_DT)

OveralllastTransaction
#######TenureDays ---- OveralllastTransaction - NominationDate ---Finding the overalllast transaction date(if missing none is put 0
TenureDays<-data.frame(OveralllastTransaction - data1$X_NOMIN_DT)
finaldf<-data1
merge(finaldf,TenureDays)
