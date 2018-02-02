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
data6<- read.delim("~/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv/data/stg_bgdt_cust_purc_app.txt", header=T, sep="\t")
data6$TRANSACTION_DATE<-as.Date(mdy_hm(data6$TRANSACTION_DATE ))
data6 <- data6[(data6$TRANSACTION_DATE > "2011-12-25") ,]
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
    (julianday('2013-04-11') - julianday(MAX(TRANSACTION_DATE))) AS RecencyApp,
    (julianday('2012-01-02') - julianday(MAX(TRANSACTION_DATE))) AS RecencyApp7,
    (julianday('2012-01-26') - julianday(MAX(TRANSACTION_DATE))) AS RecencyApp30,
    (julianday('2012-03-23') - julianday(MAX(TRANSACTION_DATE))) AS RecencyApp90,
    (julianday('2012-06-23') - julianday(MAX(TRANSACTION_DATE))) AS RecencyApp180,
    (julianday('2012-12-26') - julianday(MAX(TRANSACTION_DATE))) AS RecencyApp360,
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
      (julianday('2013-04-11') - julianday(MAX(TRANSACTION_DT))) AS RecencyLF,
      (julianday('2012-01-02') - julianday(MAX(TRANSACTION_DT))) AS RecencyLF7,
      (julianday('2012-01-26') - julianday(MAX(TRANSACTION_DT))) AS RecencyLF30,
      (julianday('2012-03-23') - julianday(MAX(TRANSACTION_DT))) AS RecencyLF90,
      (julianday('2012-06-23') - julianday(MAX(TRANSACTION_DT))) AS RecencyLF180,
      (julianday('2012-12-26') - julianday(MAX(TRANSACTION_DT))) AS RecencyLF360
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

data4<- read.delim("~/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv/data/stg_bgdt_cust_gam_actv.csv", header=T, sep=",")

data4$TITLE_NOMIN_DT<-as.Date(dmy(data4$TITLE_NOMIN_DT ))
data4 <- data4[(data4$TITLE_NOMIN_DT > "2011-12-25"),]
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
data2t <- read.delim("~/Projects/CSE7302c_CUTe01_Exam-Files/hopmonkClv/data/stg_bgdt_cust_app_dwnld.txt", header=T, sep="\t")
data2<- data2t

# colnames(data2)[which(names(data2) == "NOMIN_DT")] <- "DEVICE_DESCRI"
# colnames(data2)[which(names(data2) == "DEVICE_DESCR")] <- "NOMIN_DT"
# colnames(data2)[which(names(data2) == "DEVICE_DESCRI")] <- "DEVICE_DESCR"
# colnames(data2)[which(names(data2) == "ITEM_NUMBER")] <- "ITEM_NAMEE"
# colnames(data2)[which(names(data2) == "ITEM_NAME")] <- "ITEM_NUMBER"
# colnames(data2)[which(names(data2) == "ITEM_NAMEE")] <- "ITEM_NAME"

data2$NOMIN_DT<- as.Date(mdy_hm(data2$NOMIN_DT ))
class(data2$NOMIN_DT)

data2 <- data2[(data2$NOMIN_DT > "2011-12-25"),]

names(data2)

data2$NOMIN_DT <- as.character(data2$NOMIN_DT)

######Extracting RecencyDown (6 columns)
custAppDownload <- sqldf("SELECT CONTACT_WID, MAX(NOMIN_DT) AS OveralllastDate,
                          (julianday('2013-04-11') - julianday(MAX(NOMIN_DT))) AS Recencydown,
                          (julianday('2012-01-02') - julianday(MAX(NOMIN_DT))) AS Recencydown7,
                          (julianday('2012-01-26') - julianday(MAX(NOMIN_DT))) AS Recencydown30,
                          (julianday('2012-03-23') - julianday(MAX(NOMIN_DT))) AS Recencydown90,
                          (julianday('2012-06-23') - julianday(MAX(NOMIN_DT))) AS Recencydown180,
                          (julianday('2012-12-26') - julianday(MAX(NOMIN_DT))) AS Recencydown360
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
                   MinChildAge,
                   MaxChildAge,
                   ChildAgeRange,
                   NumHouseChildren,
                   NumMaleChildrenHousehold,
                   NumFemaleChildrenHousehold,
                   NumGamesBought,
                   FrequencyApp,
                   FrequencyApp7,
                   FrequencyApp30,
                   FrequencyApp90,
                   FrequencyApp180,
                   FrequencyApp360,
                   custPurcApp.RecencyApp AS RecencyApp,
                   custPurcApp.RecencyApp7 AS RecencyApp7,
                   custPurcApp.RecencyApp30 As RecencyApp30,
                   custPurcApp.RecencyApp90 AS RecencyApp90,
                   custPurcApp.RecencyApp180 As RecencyApp180,
                   custPurcApp.RecencyApp360 AS RecencyApp360,
                   FrequencyLF,
                   FrequencyLF7,
                   FrequencyLF30,
                   FrequencyLF90,
                   FrequencyLF180,
                   FrequencyLF360,
                   custPurcLf.RecencyLF AS RecencyLF,
                   custPurcLf.RecencyLF7 AS RecencyLF7,
                   custPurcLf.RecencyLF30 AS RecencyLF30,
                   custPurcLf.RecencyLF90 AS RecencyLF90,
                   custPurcLf.RecencyLF180 AS RecencyLF180,
                   custPurcLf.RecencyLF360 AS RecencyLF360,
                   custPurcAppLf.OveralllastTransaction AS OveralllastTransaction,
                   custPurcAppLf.UNITS AS UNITS,
                   custPurcAppLf.UNIT7 AS UNITS7,
                   custPurcAppLf.UNIT30 AS UNITS30,
                   custPurcAppLf.UNIT90 AS UNITS90,
                   custPurcAppLf.UNIT180 As UNITS180,
                   custPurcAppLf.UNIT360 As UNITS360,
                   custPurcAppLf.TotalRevenueGenerated As TotalRevenueGenerated,
                   custPurcAppLf.REVENUE7 As REVENUE7,
                   custPurcAppLf.REVENUE30 AS REVENUE30,
                   custPurcAppLf.REVENUE90 AS REVENUE90,
                   custPurcAppLf.REVENUE180 AS REVENUE180,
                   custPurcAppLf.REVENUE360 AS REVENUE360,
                   TenureDays,
                   custGamActv.FreqGamePlay AS FreqGamePlay,
                   custGamActv.FreqGamePlay7 AS FreqGamePlay7,
                   custGamActv.FreqGamePlay30 AS FreqGamePlay30,
                   custGamActv.FreqGamePlay90 AS FreqGamePlay90,
                   custGamActv.FreqGamePlay180 AS FreqGamePlay180,
                   custGamActv.FreqGamePlay360 As FreqGamePlay360,
                   custGamActv.TotalTimeGamePlay AS TotalTimeGamePlay,
                   custGamActv.TotalTimeGamePlay7 As TotalTimeGamePlay7,
                   custGamActv.TotalTimeGamePlay30 As TotalTimeGamePlay30,
                   custGamActv.TotalTimeGamePlay90 AS TotalTimeGamePlay90,
                   custGamActv.TotalTimeGamePlay180 As TotalTimeGamePlay180,
                   custGamActv.TotalTimeGamePlay360 As TotalTimeGamePlay360,
                   custGamActv.NumGamesPlayed AS NumGamesPlayed,
                   custGamActv.NumGamesPlayed7 AS NumGamesPlayed7,
                   custGamActv.NumGamesPlayed30 AS NumGamesPlayed30,
                   custGamActv.NumGamesPlayed90 AS NumGamesPlayed90,
                   custGamActv.NumGamesPlayed180 AS NumGamesPlayed180,
                   custGamActv.NumGamesPlayed360 AS NumGamesPlayed360,
                   GameStrength,
                   FavoriteGameBin,
                   FavoriteGameBin7,
                   FavoriteGameBin30,
                   FavoriteGameBin90,
                   FavoriteGameBin180,
                   FavoriteGameBin360,
                   custAppDownload.Recencydown AS Recencydown,
                   custAppDownload.Recencydown7 AS Recencydown7,
                   custAppDownload.Recencydown30 AS Recencydown30,
                   custAppDownload.Recencydown90 AS Recencydown90,
                   custAppDownload.Recencydown180 AS Recencydown180,
                   custAppDownload.Recencydown360 AS Recencydown360,
                   minRecencyCum,
                   maxRecencyCum,
                   minRecencyCum7,
                   maxRecencyCum7,
                   minRecencyCum30,
                   maxRecencyCum30,
                   minRecencyCum90,
                   maxRecencyCum90,
                   minRecencyCum180,
                   maxRecencyCum180,
                   minRecencyCum360,
                   maxRecencyCum360,
                   FavouriteSource,
                   FavouriteSource7,
                   FavouriteSource30,
                   FavouriteSource90,
                   FavouriteSource180,
                   FavouriteSource360,
                   FavouriteChannel,
                   FavouriteChannel7,
                   FavouriteChannel30,
                   FavouriteChannel90,
                   FavouriteChannel180,
                   FavouriteChannel360
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
#####################################################################################################

          
