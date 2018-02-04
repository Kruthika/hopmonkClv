rm(list=ls(all=TRUE))

###There were NAs in most data sets and I could not perform any operations on existing variables.

#reading and processing ownr.txt
data<-read.delim("stg_bgdt_cust_ownr.txt", header =T,sep="\t")

names(data)

require(lubridate)
data$X_NOMIN_DT<-as.Date(mdy_hm(data$X_NOMIN_DT))

data<-data[( data$X_NOMIN_DT > "2011-12-24" & data$X_NOMIN_DT < "2013-01-27") & (data$X_EDW_INTEGRATION_ID == "kutti" | data$X_EDW_INTEGRATION_ID == "PAD2"),]
library(sqldf)

data$X_NOMIN_DT <- as.character(data$X_NOMIN_DT)

#Checking for count of distinct values in all columns
sqldf("SELECT 
      (SELECT count(*) FROM data) as NUM_ROWS,
      (SELECT count(DISTINCT CONTACT_WID) FROM data) as CONTACT_WID,
      (SELECT count(DISTINCT X_NOMIN_DT) FROM data) as X_NOMIN_DT,
      (SELECT count(DISTINCT DEVICE_SRL_NUM) FROM data) as DEVICE_SRL_NUM,
      (SELECT count(DISTINCT ASSET_WID) FROM data) as ASSET_WID,
      (SELECT count(DISTINCT X_EDW_INTEGRATION_ID) FROM data) as X_EDW_INTEGRATION_ID,
      (SELECT count(DISTINCT X_CNTRY) FROM data) as X_CNTRY,
      (SELECT count(DISTINCT X_TYPE) FROM data) as X_TYPE")

#Extracting NominationDate and Country
custOwner <- sqldf("SELECT CONTACT_WID, min(X_NOMIN_DT) as NominationDate , min(X_CNTRY) as Country from data group by CONTACT_WID")

###########################################################################################################

data3<- read.delim("stg_bgdt_cust_chld.txt", header=T, sep="\t")

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

###Getting MinChildAge, MaxChildAge, ChildAgeRange, NumHouseChildren, NumMaleChildrenHousehold, NumFemaleChildrenHousehold
custChild <- sqldf("SELECT CONTACT_WID, 
                   min(CHILD_AGE) as MinChildAge, 
                   max(CHILD_AGE) as MaxChildAge,
                   (max(CHILD_AGE)-min(CHILD_AGE)) as ChildAgeRange,
                   SUM(CASE 
                   WHEN CRM_CHLD_KEY NOTNULL THEN 1
                   ELSE 0
                   END) AS NumHouseChildren,
                   SUM(CASE 
                   WHEN SEX_MF_CODE = 'M' THEN 1
                   ELSE 0
                   END) AS NumMaleChildrenHousehold,
                   SUM(CASE 
                   WHEN SEX_MF_CODE = 'F' THEN 1
                   ELSE 0
                   END) AS NumFemaleChildrenHousehold
                   FROM data3 GROUP BY CONTACT_WID")

##########################################################################################################
#reading and processing cust_purc_app.txt
data6<- read.delim("stg_bgdt_cust_purc_app.txt", header=T, sep="\t")
data6$TRANSACTION_DATE<-as.Date(mdy_hm(data6$TRANSACTION_DATE ))
data6 <- data6[(data6$TRANSACTION_DATE > "2011-12-25" & data6$TRANSACTION_DATE < "2013-04-11") ,]
data6$TRANSACTION_DATE = as.character(data6$TRANSACTION_DATE)
class(data6$TRANSACTION_DATE)

names(data6)
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

####Extracting NumGamesBought, all aggregations relating to FrequencyApp(6 columns), RecencyApp(6) ####

custPurcApp <- sqldf("select CONTACT_WID, MAX(TRANSACTION_DATE) AS OveralllastTransaction, 
                     SUM(CASE 
                     WHEN ITEM_NUMBER > 0 THEN 1
                     ELSE 0
                     END) AS NumGamesBought,
                     SUM(UNITS) AS UNITS,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-01-02' AND TRANSACTION_DATE NOTNULL THEN UNITS
                     ELSE 0
                     END) AS UNIT7,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-01-26' AND TRANSACTION_DATE NOTNULL THEN UNITS
                     ELSE 0
                     END) AS UNIT30,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-03-26' AND TRANSACTION_DATE NOTNULL THEN UNITS
                     ELSE 0
                     END) AS UNIT90,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-06-23' AND TRANSACTION_DATE NOTNULL THEN UNITS
                     ELSE 0
                     END) AS UNIT180,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-12-26' AND TRANSACTION_DATE NOTNULL THEN UNITS
                     ELSE 0
                     END) AS UNIT360,
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
                     END) AS FrequencyApp360,
                     SUM(AMOUNT_USD) AS REVENUE,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-01-02' AND TRANSACTION_DATE NOTNULL THEN AMOUNT_USD
                     ELSE 0
                     END) AS REVENUE7,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-01-26' AND TRANSACTION_DATE NOTNULL THEN AMOUNT_USD
                     ELSE 0
                     END) AS REVENUE30,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-03-26' AND TRANSACTION_DATE NOTNULL THEN AMOUNT_USD
                     ELSE 0
                     END) AS REVENUE90,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-06-23' AND TRANSACTION_DATE NOTNULL THEN AMOUNT_USD
                     ELSE 0
                     END) AS REVENUE180,
                     SUM(CASE 
                     WHEN TRANSACTION_DATE < '2012-12-26' AND TRANSACTION_DATE NOTNULL THEN AMOUNT_USD
                     ELSE 0
                     END) AS REVENUE360,
                     MAX(CASE 
                     WHEN TRANSACTION_DATE < '2013-04-11' AND TRANSACTION_DATE NOTNULL THEN
                     abs(julianday('2013-04-11') - julianday(TRANSACTION_DATE)) 
                     ELSE 0 
                     END) AS RecencyApp,
                     MAX(CASE
                     WHEN TRANSACTION_DATE < '2012-01-02' AND TRANSACTION_DATE NOTNULL THEN
                     abs(julianday(TRANSACTION_DATE) - julianday('2012-01-01')) 
                     ELSE 0 
                     END) AS RecencyApp7,
                     MAX(CASE
                     WHEN TRANSACTION_DATE < '2012-01-26' AND TRANSACTION_DATE NOTNULL THEN
                     abs(julianday(TRANSACTION_DATE) - julianday('2012-01-25')) 
                     ELSE 0 
                     END) AS RecencyApp30,
                     MAX(CASE
                     WHEN TRANSACTION_DATE < '2012-03-23' AND TRANSACTION_DATE NOTNULL THEN
                     abs(julianday(TRANSACTION_DATE) - julianday('2012-03-22')) 
                     ELSE 0 
                     END) AS RecencyApp90,
                     MAX(CASE
                     WHEN TRANSACTION_DATE < '2012-06-23' AND TRANSACTION_DATE NOTNULL THEN
                     abs(julianday(TRANSACTION_DATE) - julianday('2012-06-22')) 
                     ELSE 0 
                     END) AS RecencyApp180,
                     MAX(CASE
                     WHEN TRANSACTION_DATE < '2012-12-26' AND TRANSACTION_DATE NOTNULL THEN
                     abs(julianday(TRANSACTION_DATE) - julianday('2012-12-25')) 
                     ELSE 0 
                     END) AS RecencyApp360,
                     SUM(CASE
                     WHEN SOURCE_OF_PURCHASE NOTNULL THEN 1
                     ELSE 0
                     END) AS total_source_purchase,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-01-02' AND SOURCE_OF_PURCHASE NOTNULL) THEN 1
                     ELSE 0
                     END) AS total_source_purchase7,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-01-26' AND SOURCE_OF_PURCHASE NOTNULL)  THEN 1
                     ELSE 0
                     END) AS total_source_purchase30,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-03-26' AND SOURCE_OF_PURCHASE NOTNULL) THEN 1
                     ELSE 0
                     END) AS total_source_purchase90,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-06-23' AND SOURCE_OF_PURCHASE NOTNULL) THEN 1
                     ELSE 0
                     END) AS total_source_purchase180,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-12-26' AND SOURCE_OF_PURCHASE NOTNULL)  THEN 1
                     ELSE 0
                     END) AS total_source_purchase360,
                     SUM(CASE
                     WHEN CHANNEL_DESCRIPTION NOTNULL THEN 1
                     ELSE 0
                     END) AS CHANNEL_DESCRIPTION,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-01-02' AND CHANNEL_DESCRIPTION NOTNULL) THEN 1
                     ELSE 0
                     END) AS CHANNEL_DESCRIPTION7,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-01-26' AND CHANNEL_DESCRIPTION NOTNULL)  THEN 1
                     ELSE 0
                     END) AS CHANNEL_DESCRIPTION30,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-03-26' AND CHANNEL_DESCRIPTION NOTNULL) THEN 1
                     ELSE 0
                     END) AS CHANNEL_DESCRIPTION90,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-06-23' AND CHANNEL_DESCRIPTION NOTNULL) THEN 1
                     ELSE 0
                     END) AS CHANNEL_DESCRIPTION180,
                     SUM(CASE 
                     WHEN (TRANSACTION_DATE < '2012-12-26' AND CHANNEL_DESCRIPTION NOTNULL)  THEN 1
                     ELSE 0
                     END) AS CHANNEL_DESCRIPTION360
                     FROM data6
                     group by CONTACT_WID")

PurcAppSource <- sqldf(" SELECT CONTACT_WID, SOURCE_OF_PURCHASE, SUM(CASE
                       WHEN SOURCE_OF_PURCHASE NOTNULL THEN 1
                       ELSE 0
                       END) AS source_purchase,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-01-02' AND SOURCE_OF_PURCHASE NOTNULL) THEN 1
                       ELSE 0
                       END) AS source_purchase7,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-01-26' AND SOURCE_OF_PURCHASE NOTNULL)  THEN 1
                       ELSE 0
                       END) AS source_purchase30,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-03-26' AND SOURCE_OF_PURCHASE NOTNULL) THEN 1
                       ELSE 0
                       END) AS source_purchase90,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-06-23' AND SOURCE_OF_PURCHASE NOTNULL) THEN 1
                       ELSE 0
                       END) AS source_purchase180,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-12-26' AND SOURCE_OF_PURCHASE NOTNULL)  THEN 1
                       ELSE 0
                       END) AS source_purchase360
                       FROM data6
                       group by CONTACT_WID, SOURCE_OF_PURCHASE")
FavSource <- sqldf("SELECT a.CONTACT_WID, a.SOURCE_OF_PURCHASE,
                   CASE 
                   WHEN source_purchase > 0.5 * total_source_purchase THEN 1
                   ELSE NULL
                   END AS FavSource,
                   CASE 
                   WHEN source_purchase7 > 0.5 * total_source_purchase7 THEN 1
                   ELSE NULL
                   END AS FavSource7,
                   CASE 
                   WHEN source_purchase30 > 0.5 * total_source_purchase30 THEN 1
                   ELSE NULL
                   END AS FavSource30,
                   CASE 
                   WHEN source_purchase90 > 0.5 * total_source_purchase90 THEN 1
                   ELSE NULL
                   END AS FavSource90,
                   CASE 
                   WHEN source_purchase180 > 0.5 * total_source_purchase180 THEN 1
                   ELSE NULL
                   END AS FavSource180,
                   CASE
                   WHEN source_purchase360 > 0.5 * total_source_purchase180 THEN 1
                   ELSE NULL
                   END AS FavSource360
                   FROM PurcAppSource a
                   LEFT JOIN custPurcApp b on a.CONTACT_WID = b.CONTACT_WID")

FavSource1 <- sqldf("SELECT CONTACT_WID, 
                    CASE
                    WHEN FavSource = 1 THEN SOURCE_OF_PURCHASE
                    ELSE NULL
                    END AS FavSourceBin,
                    CASE
                    WHEN FavSource7 = 1 THEN SOURCE_OF_PURCHASE
                    ELSE NULL
                    END AS FavSourceBin7,
                    CASE
                    WHEN FavSource30 = 1 THEN SOURCE_OF_PURCHASE
                    ELSE NULL
                    END AS FavSourceBin30,
                    CASE
                    WHEN FavSource90 = 1 THEN SOURCE_OF_PURCHASE
                    ELSE NULL
                    END AS FavSourceBin90,
                    CASE
                    WHEN FavSource180 = 1 THEN SOURCE_OF_PURCHASE
                    ELSE NULL
                    END AS FavSourceBin180,
                    CASE
                    WHEN FavSource360 = 1 THEN SOURCE_OF_PURCHASE
                    ELSE NULL
                    END AS FavSourceBin360
                    FROM FavSource")

FavouriteSourceBin <- sqldf("SELECT CONTACT_WID,
                            max(FavSourceBin) as FavouriteSource,
                            max(FavSourceBin7) as FavouriteSource7,
                            max(FavSourceBin30) as FavouriteSource30,
                            max(FavSourceBin90) as FavouriteSource90,
                            max(FavSourceBin180) as FavouriteSource180,
                            max(FavSourceBin360) as FavouriteSource360
                            FROM FavSource1 GROUP BY CONTACT_WID")



PurAppChannel <- sqldf("SELECT CONTACT_WID, CHANNEL_DESCRIPTION,SUM(CASE
                       WHEN CHANNEL_DESCRIPTION NOTNULL THEN 1
                       ELSE 0
                       END) AS CHANNEL,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-01-02' AND CHANNEL_DESCRIPTION NOTNULL) THEN 1
                       ELSE 0
                       END) AS CHANNEL7,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-01-26' AND CHANNEL_DESCRIPTION NOTNULL)  THEN 1
                       ELSE 0
                       END) AS CHANNEL30,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-03-26' AND CHANNEL_DESCRIPTION NOTNULL) THEN 1
                       ELSE 0
                       END) AS CHANNEL90,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-06-23' AND CHANNEL_DESCRIPTION NOTNULL) THEN 1
                       ELSE 0
                       END) AS CHANNEL180,
                       SUM(CASE 
                       WHEN (TRANSACTION_DATE < '2012-12-26' AND CHANNEL_DESCRIPTION NOTNULL)  THEN 1
                       ELSE 0
                       END) AS CHANNEL360
                       FROM data6
                       group by CONTACT_WID, CHANNEL_DESCRIPTION")

FavChannel <- sqldf("SELECT a.CONTACT_WID, a.CHANNEL_DESCRIPTION,
                    CASE 
                    WHEN CHANNEL > 0.5 * b.CHANNEL_DESCRIPTION THEN 1
                    ELSE NULL
                    END AS favChannel,
                    CASE 
                    WHEN CHANNEL7 > 0.5 * b.CHANNEL_DESCRIPTION7 THEN 1
                    ELSE NULL
                    END AS favChannel7,
                    CASE 
                    WHEN CHANNEL30 > 0.5 * b.CHANNEL_DESCRIPTION30 THEN 1
                    ELSE NULL
                    END AS favChannel30,
                    CASE 
                    WHEN CHANNEL90 > 0.5 * b.CHANNEL_DESCRIPTION90 THEN 1
                    ELSE NULL
                    END AS favChannel90,
                    CASE 
                    WHEN CHANNEL180 > 0.5 * b.CHANNEL_DESCRIPTION180 THEN 1
                    ELSE NULL
                    END AS favChannel180,
                    CASE 
                    WHEN CHANNEL360 > 0.5 * b.CHANNEL_DESCRIPTION360 THEN 1
                    ELSE NULL
                    END AS favChannel360
                    FROM PurAppChannel a
                    LEFT JOIN custPurcApp b on a.CONTACT_WID = b.CONTACT_WID
                    ")



FavChannel1 <- sqldf("SELECT CONTACT_WID,
                     CASE 
                     WHEN favChannel = 1 THEN CHANNEL_DESCRIPTION
                     ELSE NULL
                     END AS favouriteChannel,
                     CASE 
                     WHEN favChannel7 = 1 THEN CHANNEL_DESCRIPTION
                     ELSE NULL
                     END AS favouriteChannel7,
                     CASE 
                     WHEN favChannel30 = 1 THEN CHANNEL_DESCRIPTION
                     ELSE NULL
                     END AS favouriteChannel30,
                     CASE 
                     WHEN favChannel90 = 1 THEN CHANNEL_DESCRIPTION
                     ELSE NULL
                     END AS favouriteChannel90,
                     CASE 
                     WHEN favChannel180 = 1 THEN CHANNEL_DESCRIPTION
                     ELSE NULL
                     END AS favouriteChannel180,
                     CASE 
                     WHEN favChannel360 = 1 THEN CHANNEL_DESCRIPTION
                     ELSE NULL
                     END AS favouriteChannel360
                     FROM FavChannel")

favouriteChannelBin <- sqldf("SELECT CONTACT_WID, max(favouriteChannel) as FavouriteChannel,
                             max(favouriteChannel7) as FavouriteChannel7,
                             max(favouriteChannel30) as FavouriteChannel30,
                             max(favouriteChannel90) as FavouriteChannel90,
                             max(favouriteChannel180) as FavouriteChannel180,
                             max(favouriteChannel360) as FavouriteChannel360
                             FROM FavChannel1 GROUP BY CONTACT_WID")



##########################################################################################################
#reading and processing cust_purc_lf.txt
data7<- read.delim("stg_bgdt_cust_purc_lf.txt", header=T, sep="\t")
data7$TRANSACTION_DT<-as.Date(dmy_hm(data7$TRANSACTION_DT ))
data7 <- data7[(data7$TRANSACTION_DT>"2011-12-25" & data7$TRANSACTION_DT < "2013-04-11") ,]
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

#Exctracting OveralllastTransaction, aggregations relating to FrequencyLF(6 columns), RecencyLF(6)
custPurcLf <- sqldf("select CONTACT_WID, max(TRANSACTION_DT) AS OveralllastTransaction,
                    SUM(UNITS) AS UNITS,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-01-02' AND TRANSACTION_DT NOTNULL THEN UNITS
                    ELSE 0
                    END) AS UNIT7,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-01-26' AND TRANSACTION_DT NOTNULL THEN UNITS
                    ELSE 0
                    END) AS UNIT30,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-03-26' AND TRANSACTION_DT NOTNULL THEN UNITS
                    ELSE 0
                    END) AS UNIT90,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-06-23' AND TRANSACTION_DT NOTNULL THEN UNITS
                    ELSE 0
                    END) AS UNIT180,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-12-26' AND TRANSACTION_DT NOTNULL THEN UNITS
                    ELSE 0
                    END) AS UNIT360,
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
                    END) AS FrequencyLF360,
                    SUM(AMOUNT) AS REVENUE,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-01-02' AND TRANSACTION_DT NOTNULL THEN AMOUNT
                    ELSE 0
                    END) AS REVENUE7,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-01-26' AND TRANSACTION_DT NOTNULL THEN AMOUNT
                    ELSE 0
                    END) AS REVENUE30,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-03-26' AND TRANSACTION_DT NOTNULL THEN AMOUNT
                    ELSE 0
                    END) AS REVENUE90,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-06-23' AND TRANSACTION_DT NOTNULL THEN AMOUNT
                    ELSE 0
                    END) AS REVENUE180,
                    SUM(CASE 
                    WHEN TRANSACTION_DT < '2012-12-26' AND TRANSACTION_DT NOTNULL THEN AMOUNT
                    ELSE 0
                    END) AS REVENUE360,
                    MAX(CASE 
                    WHEN TRANSACTION_DT < '2013-04-11' AND TRANSACTION_DT NOTNULL THEN
                    abs(julianday('2013-04-11') - julianday(TRANSACTION_DT)) 
                    ELSE 0 
                    END) AS RecencyLF,
                    MAX(CASE
                    WHEN TRANSACTION_DT < '2012-01-02' AND TRANSACTION_DT NOTNULL THEN
                    abs(julianday(TRANSACTION_DT) - julianday('2012-01-01')) 
                    ELSE 0 
                    END) AS RecencyLF7,
                    MAX(CASE
                    WHEN TRANSACTION_DT < '2012-01-26' AND TRANSACTION_DT NOTNULL THEN
                    abs(julianday(TRANSACTION_DT) - julianday('2012-01-25')) 
                    ELSE 0 
                    END) AS RecencyLF30,
                    MAX(CASE
                    WHEN TRANSACTION_DT < '2012-03-23' AND TRANSACTION_DT NOTNULL THEN
                    abs(julianday(TRANSACTION_DT) - julianday('2012-03-22')) 
                    ELSE 0 
                    END) AS RecencyLF90,
                    MAX(CASE
                    WHEN TRANSACTION_DT < '2012-06-23' AND TRANSACTION_DT NOTNULL THEN
                    abs(julianday(TRANSACTION_DT) - julianday('2012-06-22')) 
                    ELSE 0 
                    END) AS RecencyLF180,
                    MAX(CASE
                    WHEN TRANSACTION_DT < '2012-12-26' AND TRANSACTION_DT NOTNULL THEN
                    abs(julianday(TRANSACTION_DT) - julianday('2012-12-25')) 
                    ELSE 0 
                    END) AS RecencyLF360
                    FROM data7
                    group by CONTACT_WID")


##########################################################################################################

#Merging custPurcApp and custPurcLf tables to get aggregations of UNITS(6 columns), Revenue(6)


custPurcAppLf <- sqldf("SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID, 
                       COALESCE(a.OveralllastTransaction, b.OveralllastTransaction) AS OveralllastTransaction, 
                       (coalesce(a.UNITS,0) + coalesce(b.Units,0)) as UNITS, 
                       (coalesce(a.UNIT7,0) + coalesce(b.UNIT7,0)) AS UNIT7,
                       (coalesce(a.UNIT30,0) + coalesce(b.UNIT30,0)) AS UNIT30, 
                       (coalesce(a.UNIT90,0) + coalesce(b.UNIT90,0)) AS UNIT90, 
                       (coalesce(a.UNIT180,0) + coalesce(b.UNIT180,0)) AS UNIT180, 
                       (coalesce(a.UNIT360,0) + coalesce(b.UNIT360,0)) AS UNIT360,
                       (coalesce(a.REVENUE,0) + coalesce(b.REVENUE,0)) as TotalRevenueGenerated, 
                       (coalesce(a.REVENUE7,0) + coalesce(b.REVENUE7,0)) AS REVENUE7,
                       (coalesce(a.REVENUE30,0) + coalesce(b.REVENUE30,0)) AS REVENUE30, 
                       (coalesce(a.REVENUE90,0) + coalesce(b.REVENUE90,0)) AS REVENUE90, 
                       (coalesce(a.REVENUE180,0) + coalesce(b.REVENUE180,0)) AS REVENUE180, 
                       (coalesce(a.REVENUE360,0) + coalesce(b.REVENUE360,0)) AS REVENUE360,
                       coalesce(a.RecencyApp,NULL) AS RecencyApp,
                       coalesce(a.RecencyApp7,NULL) AS RecencyApp7,
                       coalesce(a.RecencyApp30,NULL) AS RecencyApp30, 
                       coalesce(a.RecencyApp90,NULL)  AS RecencyApp90, 
                       coalesce(a.RecencyApp180,NULL) AS RecencyApp180, 
                       coalesce(a.RecencyApp360,NULL)  AS RecencyApp360,
                       coalesce(b.RecencyLF,NULL) AS RecencyLF,
                       coalesce(b.RecencyLF7,NULL) AS RecencyLF7,
                       coalesce(b.RecencyLF30,NULL) AS RecencyLF30, 
                       coalesce(b.RecencyLF90,NULL)  AS RecencyLF90, 
                       coalesce(b.RecencyLF180,NULL) AS RecencyLF180, 
                       coalesce(b.RecencyLF360,NULL)  AS RecencyLF360
                       FROM custPurcApp a LEFT JOIN custPurcLf b ON a.CONTACT_WID = b.CONTACT_WID
                       UNION ALL
                       SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID, 
                       COALESCE(b.OveralllastTransaction, a.OveralllastTransaction ) AS OveralllastTransaction, 
                       (coalesce(a.UNITS,0) + coalesce(b.Units,0)) as UNITS, 
                       (coalesce(a.UNIT7,0) + coalesce(b.UNIT7,0)) AS UNIT7,
                       (coalesce(a.UNIT30,0) + coalesce(b.UNIT30,0)) AS UNIT30, 
                       (coalesce(a.UNIT90,0) + coalesce(b.UNIT90,0)) AS UNIT90, 
                       (coalesce(a.UNIT180,0) + coalesce(b.UNIT180,0)) AS UNIT180, 
                       (coalesce(a.UNIT360,0) + coalesce(b.UNIT360,0)) AS UNIT360,
                       (coalesce(a.REVENUE,0) + coalesce(b.REVENUE,0)) as TotalRevenuGenerated, 
                       (coalesce(a.REVENUE7,0) + coalesce(b.REVENUE7,0)) AS REVENUE7,
                       (coalesce(a.REVENUE30,0) + coalesce(b.REVENUE30,0)) AS REVENUE30, 
                       (coalesce(a.REVENUE90,0) + coalesce(b.REVENUE90,0)) AS REVENUE90, 
                       (coalesce(a.REVENUE180,0) + coalesce(b.REVENUE180,0)) AS REVENUE180, 
                       (coalesce(a.REVENUE360,0) + coalesce(b.REVENUE360,0)) AS REVENUE360,
                       coalesce(a.RecencyApp,NULL) AS RecencyApp,
                       coalesce(a.RecencyApp7,NULL) AS RecencyApp7,
                       coalesce(a.RecencyApp30,NULL) AS RecencyApp30, 
                       coalesce(a.RecencyApp90,NULL)  AS RecencyApp90, 
                       coalesce(a.RecencyApp180,NULL) AS RecencyApp180, 
                       coalesce(a.RecencyApp360,NULL)  AS RecencyApp360,
                       coalesce(b.RecencyLF,NULL) AS RecencyLF,
                       coalesce(b.RecencyLF7,NULL) AS RecencyLF7,
                       coalesce(b.RecencyLF30,NULL) AS RecencyLF30, 
                       coalesce(b.RecencyLF90,NULL)  AS RecencyLF90, 
                       coalesce(b.RecencyLF180,NULL) AS RecencyLF180, 
                       coalesce(b.RecencyLF360,NULL)  AS RecencyLF360
                       FROM custPurcLf b LEFT JOIN custPurcApp a ON a.CONTACT_WID = b.CONTACT_WID
                       WHERE a.CONTACT_WID IS NULL")

#########################################################################################################
#Extracting Tenure
tenure <- sqldf("SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID, OveralllastTransaction, NominationDate,
                CASE 
                WHEN NominationDate NOTNULL AND OveralllastTransaction NOTNULL THEN
                (julianday(OveralllastTransaction) -  (julianday(NominationDate)))
                ELSE 0
                END AS TenureDays 
                FROM custOwner a LEFT JOIN custPurcAppLf b ON a.CONTACT_WID = b.CONTACT_WID
                UNION ALL
                SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID,  OveralllastTransaction, NominationDate,
                CASE 
                WHEN NominationDate NOTNULL AND OveralllastTransaction NOTNULL THEN
                (julianday(OveralllastTransaction) -  (julianday(NominationDate)))
                ELSE 0
                END AS TenureDays 
                FROM custPurcAppLf b LEFT JOIN custOwner a ON a.CONTACT_WID = b.CONTACT_WID
                WHERE b.CONTACT_WID IS NULL")
######################################################################################################
##reading and processing cust_gam_actv.txt

data4<- read.delim("stg_bgdt_cust_gam_actv.csv", header=T, sep=",")

data4$TITLE_NOMIN_DT<-as.Date(dmy(data4$TITLE_NOMIN_DT ))
data4 <- data4[(data4$TITLE_NOMIN_DT > "2011-12-25" & data4$TITLE_NOMIN_DT < "2013-04-11"),]
names(data4)
data4$TITLE_NOMIN_DT <- as.character(data4$TITLE_NOMIN_DT)

#Extracting FreqGamePlay(6 columns), TotalTimeGamePlay(6) and NumGamesPlayed(6)
custGamActv <- sqldf("select CONTACT_WID, max(TITLE_NOMIN_DT) AS OveralllastTransaction,
                     SUM(ATMP_CNT) AS FreqGamePlay,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-01-02' AND TITLE_NOMIN_DT NOTNULL  THEN ATMP_CNT
                     ELSE 0
                     END) AS FreqGamePlay7,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-01-26' AND TITLE_NOMIN_DT NOTNULL THEN ATMP_CNT
                     ELSE 0
                     END) AS FreqGamePlay30,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-03-26' AND TITLE_NOMIN_DT NOTNULL THEN ATMP_CNT
                     ELSE 0
                     END) AS FreqGamePlay90,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-06-23' AND TITLE_NOMIN_DT NOTNULL THEN ATMP_CNT
                     ELSE 0
                     END) AS FreqGamePlay180,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-12-26' AND TITLE_NOMIN_DT NOTNULL THEN ATMP_CNT
                     ELSE 0
                     END) AS FreqGamePlay360,
                     SUM(ACT_TME_SPN_QTY) AS TotalTimeGamePlay,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-01-02' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                     ELSE 0
                     END) AS TotalTimeGamePlay7,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-01-26' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                     ELSE 0
                     END) AS TotalTimeGamePlay30,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-03-26' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                     ELSE 0
                     END) AS TotalTimeGamePlay90,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-06-23' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                     ELSE 0
                     END) AS TotalTimeGamePlay180,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-12-26' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                     ELSE 0
                     END) AS TotalTimeGamePlay360,
                     SUM(CASE 
                     WHEN X_GAME_NM > 0 THEN 1
                     ELSE 0
                     END) AS NumGamesPlayed,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-01-02' AND X_GAME_NM NOTNULL THEN 1
                     ELSE 0
                     END) AS NumGamesPlayed7,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-01-26' AND X_GAME_NM  NOTNULL THEN 1
                     ELSE 0
                     END) AS NumGamesPlayed30,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-03-26' AND X_GAME_NM NOTNULL THEN 1
                     ELSE 0
                     END) AS NumGamesPlayed90,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-06-23' AND X_GAME_NM NOTNULL THEN 1
                     ELSE 0
                     END) AS NumGamesPlayed180,
                     SUM(CASE 
                     WHEN TITLE_NOMIN_DT < '2012-12-26'  AND X_GAME_NM NOTNULL THEN 1
                     ELSE 0
                     END) AS NumGamesPlayed360
                     FROM data4
                     group by CONTACT_WID")


custActvPerIdGame <- sqldf("select CONTACT_WID, max(TITLE_NOMIN_DT) AS OveralllastTransaction,
                           X_GAME_NM AS GameName,
                           X_EDW_PRODUCT_NUMBER As GameId,
                           SUM(ATMP_CNT) AS FreqGamePlay,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-01-02' AND TITLE_NOMIN_DT NOTNULL  THEN ATMP_CNT
                           ELSE 0
                           END) AS FreqGamePlay7,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-01-26' AND TITLE_NOMIN_DT NOTNULL THEN ATMP_CNT
                           ELSE 0
                           END) AS FreqGamePlay30,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-03-26' AND TITLE_NOMIN_DT NOTNULL THEN ATMP_CNT
                           ELSE 0
                           END) AS FreqGamePlay90,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-06-23' AND TITLE_NOMIN_DT NOTNULL THEN ATMP_CNT
                           ELSE 0
                           END) AS FreqGamePlay180,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-12-26' AND TITLE_NOMIN_DT NOTNULL THEN ATMP_CNT
                           ELSE 0
                           END) AS FreqGamePlay360,
                           SUM(ACT_TME_SPN_QTY) AS TotalTimeGamePlay,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-01-02' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                           ELSE 0
                           END) AS TotalTimeGamePlay7,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-01-26' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                           ELSE 0
                           END) AS TotalTimeGamePlay30,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-03-26' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                           ELSE 0
                           END) AS TotalTimeGamePlay90,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-06-23' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                           ELSE 0
                           END) AS TotalTimeGamePlay180,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-12-26' AND TITLE_NOMIN_DT NOTNULL THEN ACT_TME_SPN_QTY
                           ELSE 0
                           END) AS TotalTimeGamePlay360,
                           SUM(CASE 
                           WHEN X_GAME_NM > 0 THEN 1
                           ELSE 0
                           END) AS NumGamesPlayed,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-01-02' AND X_GAME_NM NOTNULL THEN 1
                           ELSE 0
                           END) AS NumGamesPlayed7,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-01-26' AND X_GAME_NM NOTNULL THEN 1
                           ELSE 0
                           END) AS NumGamesPlayed30,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-03-26' AND X_GAME_NM NOTNULL THEN 1
                           ELSE 0
                           END) AS NumGamesPlayed90,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-06-23' AND X_GAME_NM NOTNULL THEN 1
                           ELSE 0
                           END) AS NumGamesPlayed180,
                           SUM(CASE 
                           WHEN TITLE_NOMIN_DT < '2012-12-26'  AND X_GAME_NM NOTNULL THEN 1
                           ELSE 0
                           END) AS NumGamesPlayed360
                           FROM data4
                           group by CONTACT_WID, X_EDW_PRODUCT_NUMBER")

Favorite <- sqldf("SELECT a.CONTACT_WID, a.GameId, a.GameName,
                  CASE 
                  WHEN a.TotalTimeGamePlay > 0.5 * b.TotalTimeGamePlay THEN 1
                  ELSE 0
                  END AS FavoriteGame,
                  CASE
                  WHEN a.TotalTimeGamePlay > 0.95 * b.TotalTimeGamePlay THEN 'addicted'
                  WHEN a.TotalTimeGamePlay > 0.80 * b.TotalTimeGamePlay THEN 'strongly_favourite'
                  WHEN a.TotalTimeGamePlay > 0.60 * b.TotalTimeGamePlay THEN 'favourite'
                  WHEN a.TotalTimeGamePlay > 0.40 * b.TotalTimeGamePlay THEN 'medium_favourite' 
                  WHEN a.TotalTimeGamePlay > 0.20 * b.TotalTimeGamePlay THEN 'less_favourite'
                  ELSE NULL
                  END AS GameStrength,
                  CASE 
                  WHEN a.TotalTimeGamePlay7 > 0.5 * b.TotalTimeGamePlay7 THEN 1
                  ELSE 0
                  END AS FavoriteGame7,
                  CASE 
                  WHEN a.TotalTimeGamePlay30 > 0.5 * b.TotalTimeGamePlay30 THEN 1
                  ELSE 0
                  END AS FavoriteGame30,
                  CASE 
                  WHEN a.TotalTimeGamePlay90 > 0.5 * b.TotalTimeGamePlay90 THEN 1
                  ELSE 0
                  END AS FavoriteGame90,
                  CASE 
                  WHEN a.TotalTimeGamePlay180 > 0.5 * b.TotalTimeGamePlay180 THEN 1
                  ELSE 0
                  END AS FavoriteGame180,
                  CASE 
                  WHEN a.TotalTimeGamePlay360 > 0.5 * b.TotalTimeGamePlay360 THEN 1
                  ELSE 0
                  END AS FavoriteGame360
                  FROM custActvPerIdGame a LEFT JOIN custGamActv b on a.CONTACT_WID = b.CONTACT_WID")

favoriteGame <- sqldf("SELECT CONTACT_WID, 
                      CASE 
                      WHEN FavoriteGame = 1 THEN GameId
                      ELSE NULL
                      END AS FavoriteGameBin,
                      CASE
                      WHEN FavoriteGame = 1 THEN GameStrength
                      ELSE NULL
                      END AS GameStrengthBin,
                      CASE 
                      WHEN FavoriteGame7 = 1 THEN GameId
                      ELSE NULL
                      END AS FavoriteGameBin7,
                      CASE 
                      WHEN FavoriteGame30 = 1 THEN GameId
                      ELSE NULL
                      END AS FavoriteGameBin30,
                      CASE 
                      WHEN FavoriteGame90 = 1 THEN GameId
                      ELSE NULL
                      END AS FavoriteGameBin90,
                      CASE 
                      WHEN FavoriteGame180 = 1 THEN GameId
                      ELSE NULL
                      END AS FavoriteGameBin180,
                      CASE 
                      WHEN FavoriteGame360 = 1 THEN GameId
                      ELSE NULL
                      END AS FavoriteGameBin360
                      FROM Favorite")

favoriteGameBin <- sqldf("SELECT CONTACT_WID, MAX(FavoriteGameBin) as FavoriteGameBin,
                         MAX(GameStrengthBin) as GameStrength,
                         MAX(FavoriteGameBin7) as FavoriteGameBin7,
                         MAX(FavoriteGameBin30) as FavoriteGameBin30,
                         MAX(FavoriteGameBin90) as FavoriteGameBin90,
                         MAX(FavoriteGameBin180) as FavoriteGameBin180,
                         MAX(FavoriteGameBin360) as FavoriteGameBin360
                         FROM favoriteGame group by CONTACT_WID")

#############################################################################################################################
#reading and processing cust_app_dwnld.txt
data2t <- read.delim("stg_bgdt_cust_app_dwnld.txt", header=T, sep="\t")
data2<- data2t

colnames(data2)[which(names(data2) == "NOMIN_DT")] <- "DEVICE_DESCRI"
 colnames(data2)[which(names(data2) == "DEVICE_DESCR")] <- "NOMIN_DT"
 colnames(data2)[which(names(data2) == "DEVICE_DESCRI")] <- "DEVICE_DESCR"
 colnames(data2)[which(names(data2) == "ITEM_NUMBER")] <- "ITEM_NAMEE"
 colnames(data2)[which(names(data2) == "ITEM_NAME")] <- "ITEM_NUMBER"
 colnames(data2)[which(names(data2) == "ITEM_NAMEE")] <- "ITEM_NAME"

data2$NOMIN_DT<- as.Date(mdy_hm(data2$NOMIN_DT ))
class(data2$NOMIN_DT)

data2 <- data2[(data2$NOMIN_DT > "2011-12-25" & data2$NOMIN_DT < "2013-04-11"),]

names(data2)

data2$NOMIN_DT <- as.character(data2$NOMIN_DT)

######Extracting RecencyDown (6 columns)
custAppDownload <- sqldf("SELECT CONTACT_WID, MAX(NOMIN_DT) AS OveralllastDate,
                         MAX(CASE 
                         WHEN NOMIN_DT < '2013-04-11' AND NOMIN_DT NOTNULL THEN
                         (julianday('2013-04-11') - julianday(NOMIN_DT)) 
                         ELSE 0 
                         END) AS Recencydown,
                         MAX(CASE
                         WHEN NOMIN_DT < '2012-01-02' AND NOMIN_DT NOTNULL THEN
                         abs(julianday(NOMIN_DT) - julianday('2012-01-01')) 
                         ELSE 0 
                         END) AS Recencydown7,
                         MAX(CASE
                         WHEN NOMIN_DT < '2012-01-26' AND NOMIN_DT NOTNULL THEN
                         abs(julianday(NOMIN_DT) - julianday('2012-01-25')) 
                         ELSE 0 
                         END) AS Recencydown30,
                         MAX(CASE
                         WHEN NOMIN_DT < '2012-03-23' AND NOMIN_DT NOTNULL THEN
                         abs(julianday(NOMIN_DT) - julianday('2012-03-22')) 
                         ELSE 0 
                         END) AS Recencydown90,
                         MAX(CASE
                         WHEN NOMIN_DT < '2012-06-23' AND NOMIN_DT NOTNULL THEN
                         abs(julianday(NOMIN_DT) - julianday('2012-06-22')) 
                         ELSE 0 
                         END) AS Recencydown180,
                         MAX(CASE
                         WHEN NOMIN_DT < '2012-12-26' AND NOMIN_DT NOTNULL THEN
                         abs(julianday(NOMIN_DT) - julianday('2012-12-25')) 
                         ELSE 0 
                         END) AS Recencydown360
                         FROM data2
                         group by CONTACT_WID")


########################################################################################################

#Extracting aggregated Recency columns(6)
custAll <- sqldf("SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID, 
                 COALESCE(a.OveralllastTransaction, b.OveralllastDate) AS OveralllastTransaction, 
                 coalesce(b.RecencyDown, NULL) AS RecencyDown,
                 coalesce(b.RecencyDown7,NULL) AS RecencyDown7,
                 coalesce(b.RecencyDown30, NULL) AS RecencyDown30, 
                 coalesce(b.RecencyDown90, NULL)  AS RecencyDown90, 
                 coalesce(b.RecencyDown180,NULL) AS RecencyDown180, 
                 coalesce(b.RecencyDown360,NULL)  AS RecencyDown360,
                 coalesce(a.RecencyApp,NULL) AS RecencyApp,
                 coalesce(a.RecencyApp7,NULL) AS RecencyApp7,
                 coalesce(a.RecencyApp30,NULL) AS RecencyApp30, 
                 coalesce(a.RecencyApp90,NULL)  AS RecencyApp90, 
                 coalesce(a.RecencyApp180,NULL) AS RecencyApp180, 
                 coalesce(a.RecencyApp360,NULL)  AS RecencyApp360,
                 coalesce(a.RecencyLF,NULL) AS RecencyLF,
                 coalesce(a.RecencyLF7,NULL) AS RecencyLF7,
                 coalesce(a.RecencyLF30,NULL) AS RecencyLF30, 
                 coalesce(a.RecencyLF90,NULL)  AS RecencyLF90, 
                 coalesce(a.RecencyLF180,NULL) AS RecencyLF180, 
                 coalesce(a.RecencyLF360,NULL)  AS RecencyLF360
                 FROM custPurcAppLf a LEFT JOIN custAppDownload b ON a.CONTACT_WID = b.CONTACT_WID
                 UNION ALL
                 SELECT COALESCE(a.CONTACT_WID,b.CONTACT_WID) AS CONTACT_WID, 
                 COALESCE(b.OveralllastDate, a.OveralllastTransaction ) AS OveralllastTransaction, 
                 coalesce(b.RecencyDown, NULL) AS RecencyDown,
                 coalesce(b.RecencyDown7,NULL) AS RecencyDown7,
                 coalesce(b.RecencyDown30, NULL) AS RecencyDown30, 
                 coalesce(b.RecencyDown90, NULL)  AS RecencyDown90, 
                 coalesce(b.RecencyDown180,NULL) AS RecencyDown180, 
                 coalesce(b.RecencyDown360,NULL)  AS RecencyDown360,
                 coalesce(a.RecencyApp,NULL) AS RecencyApp,
                 coalesce(a.RecencyApp7,NULL) AS RecencyApp7,
                 coalesce(a.RecencyApp30,NULL) AS RecencyApp30, 
                 coalesce(a.RecencyApp90,NULL)  AS RecencyApp90, 
                 coalesce(a.RecencyApp180,NULL) AS RecencyApp180, 
                 coalesce(a.RecencyApp360,NULL)  AS RecencyApp360,
                 coalesce(a.RecencyLF,NULL) AS RecencyLF,
                 coalesce(a.RecencyLF7,NULL) AS RecencyLF7,
                 coalesce(a.RecencyLF30,NULL) AS RecencyLF30, 
                 coalesce(a.RecencyLF90,NULL)  AS RecencyLF90, 
                 coalesce(a.RecencyLF180,NULL) AS RecencyLF180, 
                 coalesce(a.RecencyLF360,NULL)  AS RecencyLF360
                 FROM custAppDownload b LEFT JOIN custPurcAppLf a ON a.CONTACT_WID = b.CONTACT_WID
                 WHERE a.CONTACT_WID IS NULL")

##########Grouping by CONTACT_WID to get min and max Recency (6 columns)

recencyCum <- sqldf("SELECT CONTACT_WID, OveralllastTransaction,
                    CASE
                    WHEN RecencyApp < RecencyLF AND RecencyApp < RecencyDown THEN  RecencyApp
                    WHEN RecencyLF < RecencyApp AND RecencyLF < RecencyDown THEN RecencyLF
                    ELSE RecencyDown
                    END AS minRecencyCum,
                    CASE
                    WHEN RecencyApp7 < RecencyLF7 AND RecencyApp7 < RecencyDown7 THEN  RecencyApp7
                    WHEN RecencyLF7 < RecencyApp7 AND RecencyLF7 < RecencyDown7 THEN RecencyLF7
                    ELSE RecencyDown7
                    END AS minRecencyCum7,
                    CASE
                    WHEN RecencyApp30 < RecencyLF30 AND RecencyApp30 < RecencyDown30 THEN  RecencyApp30
                    WHEN RecencyLF30 < RecencyApp30 AND RecencyLF30 < RecencyDown30 THEN RecencyLF30
                    ELSE RecencyDown
                    END AS minRecencyCum30,
                    CASE
                    WHEN RecencyApp90 < RecencyLF90 AND RecencyApp90 < RecencyDown90 THEN  RecencyApp90
                    WHEN RecencyLF90 < RecencyApp90 AND RecencyLF90 < RecencyDown90 THEN RecencyLF90
                    ELSE RecencyDown90
                    END AS minRecencyCum90,
                    CASE
                    WHEN RecencyApp180 < RecencyLF180 AND RecencyApp180 < RecencyDown180 THEN  RecencyApp180
                    WHEN RecencyLF180 < RecencyApp180 AND RecencyLF180 < RecencyDown180 THEN RecencyLF180
                    ELSE RecencyDown180
                    END AS minRecencyCum180,
                    CASE
                    WHEN RecencyApp360 < RecencyLF360 AND RecencyApp360 < RecencyDown360 THEN  RecencyApp360
                    WHEN RecencyLF360 < RecencyApp360 AND RecencyLF360 < RecencyDown360 THEN RecencyLF360
                    ELSE RecencyDown360
                    END AS minRecencyCum360,
                    CASE
                    WHEN RecencyApp > RecencyLF AND RecencyApp > RecencyDown THEN  RecencyApp
                    WHEN RecencyLF > RecencyApp AND RecencyLF > RecencyDown THEN RecencyLF
                    ELSE RecencyDown
                    END AS maxRecencyCum,
                    CASE
                    WHEN RecencyApp7 > RecencyLF7 AND RecencyApp7 > RecencyDown7 THEN  RecencyApp7
                    WHEN RecencyLF7 > RecencyApp7 AND RecencyLF7 > RecencyDown7 THEN RecencyLF7
                    ELSE RecencyDown7
                    END AS maxRecencyCum7,
                    CASE
                    WHEN RecencyApp30 > RecencyLF30 AND RecencyApp30 > RecencyDown30 THEN  RecencyApp30
                    WHEN RecencyLF30 > RecencyApp30 AND RecencyLF30 > RecencyDown30 THEN RecencyLF30
                    ELSE RecencyDown30
                    END AS maxRecencyCum30,
                    CASE
                    WHEN RecencyApp90 > RecencyLF90 AND RecencyApp90 > RecencyDown90 THEN  RecencyApp90
                    WHEN RecencyLF90 > RecencyApp90 AND RecencyLF90 > RecencyDown90 THEN RecencyLF90
                    ELSE RecencyDown90
                    END AS maxRecencyCum90,
                    CASE
                    WHEN RecencyApp180 > RecencyLF180 AND RecencyApp180 > RecencyDown180 THEN  RecencyApp180
                    WHEN RecencyLF180 > RecencyApp180 AND RecencyLF180 > RecencyDown180 THEN RecencyLF180
                    ELSE RecencyDown180
                    END AS maxRecencyCum180,
                    CASE
                    WHEN RecencyApp360 > RecencyLF360 AND RecencyApp360 > RecencyDown360 THEN  RecencyApp360
                    WHEN RecencyLF360 > RecencyApp360 AND RecencyLF360 > RecencyDown360 THEN RecencyLF360
                    ELSE RecencyDown360
                    END AS maxRecencyCum360
                    FROM custAll")

###############################################################################################

###Final dataset after merge

hopmonkClv = sqldf("SELECT
                   custOwner.CONTACT_WID AS CONTACT_WID,
                   custOwner.NominationDate AS NominationDate,
                   Country,
                   COALESCE(MinChildAge,0) AS MinChildAge,
                   COALESCE(MaxChildAge,0) AS MaxChildAge,
                   COALESCE(ChildAgeRange,0) AS ChildAgeRange,
                   COALESCE(NumHouseChildren,0) AS NumHouseChildren,
                   COALESCE(NumMaleChildrenHousehold,0) AS NumMaleChildrenHousehold,
                   COALESCE(NumFemaleChildrenHousehold,0) AS NumFemaleChildrenHousehold,
                   COALESCE(NumGamesBought,0) AS NumGamesBought,
                   COALESCE(FrequencyApp,0) AS FrequencyApp,
                   COALESCE(FrequencyApp7,0) AS FrequencyApp7,
                   COALESCE(FrequencyApp30,0) AS FrequencyApp30,
                   COALESCE(FrequencyApp90,0) AS FrequencyApp90,
                   COALESCE(FrequencyApp180,0) AS FrequencyApp180,
                   COALESCE(FrequencyApp360,0) AS FrequencyApp360,
                   COALESCE(custPurcApp.RecencyApp,0) AS RecencyApp,
                   COALESCE(custPurcApp.RecencyApp7,0) AS RecencyApp7,
                   COALESCE(custPurcApp.RecencyApp30,0) As RecencyApp30,
                   COALESCE(custPurcApp.RecencyApp90,0) AS RecencyApp90,
                   COALESCE(custPurcApp.RecencyApp180,0) As RecencyApp180,
                   COALESCE(custPurcApp.RecencyApp360,0) AS RecencyApp360,
                   COALESCE(FrequencyLF,0) AS FrequencyLF,
                   COALESCE(FrequencyLF7,0) AS FrequencyLF7,
                   COALESCE(FrequencyLF30,0) AS FrequencyLF30,
                   COALESCE(FrequencyLF90,0) AS FrequencyLF90,
                   COALESCE(FrequencyLF180,0) AS FrequencyLF180,
                   COALESCE(FrequencyLF360,0) AS FrequencyLF360,
                   COALESCE(custPurcLf.RecencyLF,0) AS RecencyLF,
                   COALESCE(custPurcLf.RecencyLF7,0) AS RecencyLF7,
                   COALESCE(custPurcLf.RecencyLF30,0) AS RecencyLF30,
                   COALESCE(custPurcLf.RecencyLF90,0) AS RecencyLF90,
                   COALESCE(custPurcLf.RecencyLF180,0) AS RecencyLF180,
                   COALESCE(custPurcLf.RecencyLF360,0) AS RecencyLF360,
                   COALESCE(custPurcAppLf.OveralllastTransaction,'None') AS OveralllastTransaction,
                   COALESCE(custPurcAppLf.UNITS,0) AS UNITS,
                   COALESCE(custPurcAppLf.UNIT7,0) AS UNITS7,
                   COALESCE(custPurcAppLf.UNIT30,0) AS UNITS30,
                   COALESCE(custPurcAppLf.UNIT90,0) AS UNITS90,
                   COALESCE(custPurcAppLf.UNIT180,0) AS UNITS180,
                   COALESCE(custPurcAppLf.UNIT360,0) AS UNITS360,
                   COALESCE(custPurcAppLf.TotalRevenueGenerated,0) AS TotalRevenueGenerated,
                   COALESCE(custPurcAppLf.REVENUE7,0) AS REVENUE7,
                   COALESCE(custPurcAppLf.REVENUE30,0) AS REVENUE30,
                   COALESCE(custPurcAppLf.REVENUE90,0) AS REVENUE90,
                   COALESCE(custPurcAppLf.REVENUE180,0) AS REVENUE180,
                   COALESCE(custPurcAppLf.REVENUE360,0) AS REVENUE360,
                   COALESCE(TenureDays,0) AS TenureDays,
                   COALESCE(custGamActv.FreqGamePlay,0) AS FreqGamePlay,
                   COALESCE(custGamActv.FreqGamePlay7,0) AS FreqGamePlay7,
                   COALESCE(custGamActv.FreqGamePlay30,0) AS FreqGamePlay30,
                   COALESCE(custGamActv.FreqGamePlay90,0) AS FreqGamePlay90,
                   COALESCE(custGamActv.FreqGamePlay180,0) AS FreqGamePlay180,
                   COALESCE(custGamActv.FreqGamePlay360,0) AS FreqGamePlay360,
                   COALESCE(custGamActv.TotalTimeGamePlay,0) AS TotalTimeGamePlay,
                   COALESCE(custGamActv.TotalTimeGamePlay7,0) AS TotalTimeGamePlay7,
                   COALESCE(custGamActv.TotalTimeGamePlay30,0) AS TotalTimeGamePlay30,
                   COALESCE(custGamActv.TotalTimeGamePlay90,0) AS TotalTimeGamePlay90,
                   COALESCE(custGamActv.TotalTimeGamePlay180,0) AS TotalTimeGamePlay180,
                   COALESCE(custGamActv.TotalTimeGamePlay360,0) AS TotalTimeGamePlay360,
                   COALESCE(custGamActv.NumGamesPlayed,0) AS NumGamesPlayed,
                   COALESCE(custGamActv.NumGamesPlayed7,0) AS NumGamesPlayed7,
                   COALESCE(custGamActv.NumGamesPlayed30,0) AS NumGamesPlayed30,
                   COALESCE(custGamActv.NumGamesPlayed90,0) AS NumGamesPlayed90,
                   COALESCE(custGamActv.NumGamesPlayed180,0) AS NumGamesPlayed180,
                   COALESCE(custGamActv.NumGamesPlayed360,0) AS NumGamesPlayed360,
                   COALESCE(GameStrength, 'Least Favourtie') AS GameStrength,
                   COALESCE(FavoriteGameBin, 'None') AS FavoriteGameBin,
                   COALESCE(FavoriteGameBin7,'None') AS FavoriteGameBin7,
                   COALESCE(FavoriteGameBin30,'None') AS FavoriteGameBin30,
                   COALESCE(FavoriteGameBin90,'None') AS FavoriteGameBin90,
                   COALESCE(FavoriteGameBin180,'None') AS FavoriteGameBin180,
                   COALESCE(FavoriteGameBin360,'None') AS FavoriteGameBin360,
                   COALESCE(custAppDownload.Recencydown,0) AS Recencydown,
                   COALESCE(custAppDownload.Recencydown7,0) AS Recencydown7,
                   COALESCE(custAppDownload.Recencydown30,0) AS Recencydown30,
                   COALESCE(custAppDownload.Recencydown90,0) AS Recencydown90,
                   COALESCE(custAppDownload.Recencydown180,0) AS Recencydown180,
                   COALESCE(custAppDownload.Recencydown360,0) AS Recencydown360,
                   COALESCE(minRecencyCum,0) AS minRecencyCum,
                   COALESCE(maxRecencyCum,0) AS maxRecencyCum,
                   COALESCE(minRecencyCum7,0) AS minRecencyCum7,
                   COALESCE(maxRecencyCum7,0) AS maxRecencyCum7,
                   COALESCE(minRecencyCum30,0) AS minRecencyCum30,
                   COALESCE(maxRecencyCum30,0) AS maxRecencyCum30,
                   COALESCE(minRecencyCum90,0) AS minRecencyCum90,
                   COALESCE(maxRecencyCum90,0) AS maxRecencyCum90,
                   COALESCE(minRecencyCum180,0) AS minRecencyCum180,
                   COALESCE(maxRecencyCum180,0) AS maxRecencyCum180,
                   COALESCE(minRecencyCum360,0) AS minRecencyCum360,
                   COALESCE(maxRecencyCum360,0) AS maxRecencyCum360,
                   COALESCE(FavouriteSource,'None') AS FavouriteSource,
                   COALESCE(FavouriteSource7,'None') AS FavouriteSource7,
                   COALESCE(FavouriteSource30,'None') AS FavouriteSource30,
                   COALESCE(FavouriteSource90,'None') AS FavouriteSource90,
                   COALESCE(FavouriteSource180,'None') AS FavouriteSource180,
                   COALESCE(FavouriteSource360,'None') AS FavouriteSource360,
                   COALESCE(FavouriteChannel,'None') AS FavouriteChannel,
                   COALESCE(FavouriteChannel7,'None') AS FavouriteChannel7,
                   COALESCE(FavouriteChannel30,'None') AS FavouriteChannel30,
                   COALESCE(FavouriteChannel90,'None') AS FavouriteChannel90,
                   COALESCE(FavouriteChannel180,'None') AS FavouriteChannel180,
                   COALESCE(FavouriteChannel360,'None') AS FavouriteChannel360
                   FROM custOwner 
                   LEFT JOIN custChild on custOwner.CONTACT_WID = custChild.CONTACT_WID
                   LEFT JOIN custPurcApp on  custOwner.CONTACT_WID = custPurcApp.CONTACT_WID
                   LEFT JOIN custPurcLf on custOwner.CONTACT_WID = custPurcLf.CONTACT_WID
                   LEFT JOIN custPurcAppLf on custOwner.CONTACT_WID = custPurcAppLf.CONTACT_WID
                   LEFT JOIN tenure on custOwner.CONTACT_WID = tenure.CONTACT_WID
                   LEFT JOIN custGamActv on custOwner.CONTACT_WID = custGamActv.CONTACT_WID
                   LEFT JOIN favoriteGameBin on custOwner.CONTACT_WID = favoriteGameBin.CONTACT_WID
                   LEFT JOIN custAppDownload on custOwner.CONTACT_WID = custAppDownload.CONTACT_WID
                   LEFT JOIN recencyCum on custOwner.CONTACT_WID = recencyCum.CONTACT_WID
                   LEFT JOIN FavouriteSourceBin on custOwner.CONTACT_WID = FavouriteSourceBin.CONTACT_WID
                   LEFT JOIN favouriteChannelBin on custOwner.CONTACT_WID = favouriteChannelBin.CONTACT_WID
                   ")
#Saving to csv
write.csv(hopmonkClv, file = "hopmonkClv.csv")

# Saving on object in RData format
save(hopmonkClv, file = "hopmonkClv.RData")






hopmonk<- subset(hopmonkClv, select=-c(NominationDate,
                                       Country,
                                       OveralllastTransaction,
                                       GameStrength,FavouriteSource, FavouriteSource7,FavouriteSource30,
                                       FavouriteSource90,FavouriteSource180,FavouriteSource360,FavouriteChannel,FavouriteChannel,
                                       FavouriteChannel7,FavouriteChannel30,FavouriteChannel90,
                                       FavouriteChannel180,FavouriteChannel360,FavoriteGameBin,
                                       FavoriteGameBin7,FavoriteGameBin30,FavoriteGameBin90,
                                       FavoriteGameBin180, FavoriteGameBin360))
hopmonk1 <- decostand(hopmonk,method = "standard")

#summary(cov)
R<-cor(hopmonk1$TotalRevenueGenerated,hopmonk1)
R2<- R*R


#Loading data
hopmonkClv <- read.csv("hopmonkClv.csv")

library(vegan)
#hopmonkClv$NominationDate

hopmonkClvM<- subset(hopmonkClv, select=c(MinChildAge,NumGamesBought,FrequencyApp360,RecencyApp,RecencyApp180,RecencyApp360,
                                          FrequencyLF30,FrequencyLF360,RecencyLF,UNITS,UNITS180,UNITS360,REVENUE360,TenureDays,
                                          FreqGamePlay,FreqGamePlay360,NumGamesPlayed,NumGamesPlayed360,Recencydown,Recencydown30,
                                          Recencydown90,maxRecencyCum,maxRecencyCum360,MaxChildAge,FrequencyApp180,maxRecencyCum180,
                                          NumGamesPlayed90,FrequencyLF180,FrequencyApp180,TotalRevenueGenerated))

##### sampling the data
library(vegan)

#install.packages('caTools')
library(caTools)
set.seed(88)

sample <- sample.split(hopmonkClvM, SplitRatio = 0.70)
#sample<- subset(train, select=-c())


#get training and test data
#sample = sample.split(data$anycolumn, SplitRatio = .75)
train_a = subset(hopmonkClvM, sample == TRUE)
test_a  = subset(hopmonkClvM, sample == FALSE)
names(train)






#linear model
LinReg1 = lm(TotalRevenueGenerated~.,data = train_a)
par(mfrow=c(2,2))
plot(LinReg1)
par(mfrow=c(1,1))

#coefficients(linmodel)
summary(LinReg1)
#coefficients(linmodel)[1]

names(coefficients(LinReg1))
#To extract the residuals:
#linmodel$residuals
#To extract the train predictions:
#linmodel$fitted.values
#______________________
#plot(test$FrequencyApp,test$TotalRevenueGenerated)
#abline(train$FrequencyApp,train$TotalRevenueGenerated)
#Check for validity of linear regression assumptions ------------------##
par(mfrow = c(2,2))

plot(LinReg1,which=4)
par(mfrow=c(1,1))
hist(LinReg1$residuals)
target<-test_aS$TotalRevenueGenerated

pred<-data.frame(predict(LinReg1,test_a))
resd<-resid(LinReg1)
#Error metrics evaluation on train data and test data
library(DMwR)
require(DMwR)  
#Error verification on train data
regr.eval(train_nS$TotalRevenueGenerated, LinReg1$fitted.values)
#Error verification on test data
Pred<- regr.eval(test_a$TotalRevenueGenerated, pred)
Pred


####################################################

####################################################
#Scaling the data##########################################
library(vegan)
str(hopmonkClv)
hopmonkClvM <- decostand(hopmonkClvM,method = "standard")

dataforscaling<-subset(hopmonkClvM,select=-c(TotalRevenueGenerated))

data_num_std <- subset(hopmonkClvM)

# Apply standardization. Ensure , you exclude the target variable 
#during standardization



#Combine standardized attributes back with the 
# categorical attributes
data_std_final <- cbind(data_num_std,data_cat,hopmonkClvM['TotalRevenueGenerated'])

#Split the data into train(70%) and test data sets
set.seed(88)

sample <- sample.split(hopmonkClvM, SplitRatio = 0.70)
#sample<- subset(train, select=-c())


#get training and test data
#sample = sample.split(data$anycolumn, SplitRatio = .75)
train = subset(hopmonkClvM, sample == TRUE)
test  = subset(hopmonkClvM, sample == FALSE)



train<- subset(train, select= -c(RecencyApp360,FrequencyLF30,MaxChildAge,FrequencyApp180.1,FrequencyLF180,NumGamesPlayed90,
                                FrequencyApp180,UNITS180,RecencyApp180))


test<- subset(test, select=-c(
  RecencyApp360,FrequencyLF30,MaxChildAge,FrequencyApp180.1,FrequencyLF180,NumGamesPlayed90,
  FrequencyApp180,UNITS180,RecencyApp180))
# Build linear regression with all attributes
LinReg_std1 <- lm(TotalRevenueGenerated~., data= train)
summary(LinReg_std1)

target1<-test$TotalRevenueGenerated
pred1<-data.frame(predict(LinReg_std1,test))

#Error verification on train data
regr.eval(train$TotalRevenueGenerated, LinReg_std1$fitted.values)
#Error verification on test data
Pred<- regr.eval(test$TotalRevenueGenerated, pred1)
Pred


# Check for multicollinearity 

# 1. VIF: (check attributes with high VIF value)
library(car)
alias( LinReg_std1 )
vif(LinReg1)
str(train_nS)
# remove the highly correlated attributes and 
LinReg_std2=lm(TotalRevenueGenerated~.-MinChildAge-maxRecencyCum180 , data=train)
# build the model 
par(mfrow=c(3,3))
#scatterplot(train_nS$TotalRevenueGenerated,train_nS$Recencydown90 )
#scatterplot(train_nS$TotalRevenueGenerated,train_nS$maxRecencyCum )
#scatterplot(train_nS$TotalRevenueGenerated,train_nS$Recencydown )
#scatterplot(train_nS$TotalRevenueGenerated,train_nS$Recencydown30 )
#scatterplot(train_nS$TotalRevenueGenerated,train_nS$maxRecencyCum360 )
summary(LinReg_std2)
vif(LinReg_std2)

# 2. Stepwise Regression
library(MASS)
Step1 <- stepAIC(LinReg_std1, direction="backward")
Step2 <- stepAIC(LinReg1, direction="forward")
Step3 <- stepAIC(LinReg_std1, direction="both")
summary(Step3)


# select the final list of variables
# and build the model
Mass_LinReg1 <- lm(TotalRevenueGenerated~.-REVENUE360-UNITS360, data = train)
summary(Mass_LinReg1)
par(mfrow=c(2,2))
plot(Mass_LinReg1)
plot(Mass_LinReg1,which=4)
par(mfrow=c(1,1))
head(train)

# Identify the outliers using the cook's distance
# remove them
# build model without the influencial points (record #2729) 
data_ou<-data.frame(which(rownames(train)%in%c(26989)))
out<-train_nS[18894,]

summary(LinReg_No_infl)

train_rmoutliers <- train[which(train$MinChildAge < 0.3228014  ), ]
LinReg_No_infl<-lm(TotalRevenueGenerated~.,data = train_rmoutliers)
summary(LinReg_No_infl)

par(mfrow=c(2,2))
plot(LinReg_No_infl)
plot(LinReg_No_infl,which=4)
par(mfrow=c(1,1))


#Error verification on train data
regr.eval(train_rmoutliers$TotalRevenueGenerated, LinReg_No_infl$fitted.values) 
#Error verification on test data
MASS_Pred1<-predict(LinReg_No_infl,test)
regr.eval(test$TotalRevenueGenerated, MASS_Pred1)

Error_calc = data.frame(train_rmoutliers$TotalRevenueGenerated,LinReg_No_infl$fitted.values)

