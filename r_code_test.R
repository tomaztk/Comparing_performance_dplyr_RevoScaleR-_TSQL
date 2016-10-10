setwd('C:/DataTK')

library(RODBC)
library(dplyr)

myconn <-odbcDriverConnect("driver={SQL Server};Server=SICN-KASTRUN;database=WideWorldImportersDW;trusted_connection=true")

cust.data <- sqlQuery(myconn, "SELECT 
		fs.[City Key]
                      ,c.City
                      ,c.[Sales Territory] AS SalesTerritory
                      ,fs.[Customer Key] AS CustomerKey
                      ,fs.[Invoice Date Key]
                      ,fs.[Quantity]
                      ,fs.[Unit Price]
                      ,fs.[Tax Amount]
                      ,fs.[Total Including Tax]
                      ,fs.Profit
                      FROM [Fact].[Sale] AS  fs
                      JOIN dimension.city AS c
                      ON c.[City Key] = fs.[City Key]")

close(myconn) 


str(cust.data)

cust.data <- tbl_df(cust.data)

  
cust.data %>%
  filter('Sales Territory' != "Southeast" , 'Customer Key' != 0 , Profit <= 1000)



library(RevoScaleR)

rxDataStep(inData = cust.data,outFile = "cust.data.xdf", overwrite = TRUE, rowsPerRead = 10000)


cust_data_filter <- rxDataStep(inData = "cust.data.xdf", outFile="Cust_data_filter.xdf", overwrite=TRUE,
                               rowSelection = (SalesTerritory != "Southeast" & CustomerKey != 0 & Profit <= 10000)
                              )


r <- rxXdfToDataFrame(cust_data_filter)


library(RevoScaleR)
rxSetComputeContext("local")
rxDataStep(inData = cust.data,outFile = "cust_data.xdf", overwrite = TRUE, rowsPerRead = 10000)
dataset <- rxDataStep(inData = "cust_data.xdf", outFile="Cust_data_filter.xdf", overwrite=TRUE, rowSelection = (SalesTerritory != "Southeast" & CustomerKey != 0 & Profit <= 10000))
OutputDataSet <- rxXdfToDataFrame(dataset)



##################

setwd('C:/DataTK')

library(RODBC)
library(dplyr)

myconn <-odbcDriverConnect("driver={SQL Server};Server=SICN-KASTRUN;database=WideWorldImportersDW;trusted_connection=true")

cust.data <- sqlQuery(myconn, "SELECT 
c.City
                      ,c.[Sales Territory] AS SalesTerritory
                      ,fs.[Customer Key] AS CustomerKey
                      ,fs.[Total Including Tax] AS TotalIncludingTax
                      ,fs.Profit
                      FROM [Fact].[Sale] AS  fs
                      JOIN dimension.city AS c
                      ON c.[City Key] = fs.[City Key]")

close(myconn) 

cust.data <- tbl_df(cust.data)

cust.data %>%
  mutate(Turnover2profit=TotalIncludingTax/Profit) %>%
  group_by(City, SalesTerritory)   %>%
  filter(SalesTerritory != "Southeast", CustomerKey != 0, Profit <= 1000) %>%
  summarise(
    
     Total_turnover = sum(TotalIncludingTax, na.rm=TRUE)
     ,Max_turnover = max(TotalIncludingTax, na.rm=TRUE)
     ,Min_turnover = min(TotalIncludingTax, na.rm=TRUE)
     ,Median_turnover = median(TotalIncludingTax, na.rm=TRUE)
     ,Var_turnover = var(TotalIncludingTax, na.rm=TRUE)
     ,stdev_turnover = sd(TotalIncludingTax, na.rm=TRUE)
     ,KPI_avg_Turnover2profit = mean(Turnover2profit, na.rm=TRUE)
    
  )




library(RevoScaleR)
devtools::install_github("RevolutionAnalytics/dplyrXdf")
library(dplyrXdf)

custXdf <- rxDataFrameToXdf(cust.data, outFile="Cust_data_aggregate.xdf", overwrite=TRUE)
custagg <- custXdf %>%
  mutate(Turnover2profit=TotalIncludingTax/Profit) %>%
  group_by(City, SalesTerritory)   %>%
  filter(SalesTerritory != "Southeast", CustomerKey != 0, Profit <= 1000) %>%
  summarise(
    
    Total_turnover = sum(TotalIncludingTax, na.rm=TRUE)
    ,Max_turnover = max(TotalIncludingTax, na.rm=TRUE)
    ,Min_turnover = min(TotalIncludingTax, na.rm=TRUE)
    ,Median_turnover = median(TotalIncludingTax, na.rm=TRUE)
    ,Var_turnover = var(TotalIncludingTax, na.rm=TRUE)
    ,stdev_turnover = sd(TotalIncludingTax, na.rm=TRUE)
    ,KPI_avg_Turnover2profit = mean(Turnover2profit, na.rm=TRUE)
    
  )

head(custagg)






###################################
### 
### REvoScaleR data manipulation
###
###################################
setwd('C:/DataTK')

library(RODBC)
myconn <-odbcDriverConnect("driver={SQL Server};Server=SICN-KASTRUN;database=WideWorldImportersDW;trusted_connection=true")
cust.data <- sqlQuery(myconn, "SELECT 
                              c.City
                      ,c.[Sales Territory] AS SalesTerritory
                      ,fs.[Customer Key] AS CustomerKey
                      ,fs.[Total Including Tax] AS TotalIncludingTax
                      ,fs.Profit
                      FROM [Fact].[Sale] AS  fs
                      JOIN dimension.city AS c
                      ON c.[City Key] = fs.[City Key]")

close(myconn) 


library(RevoScaleR)


sales_rx1 <- rxDataStep(inData = cust.data, outFile="Cust_data_rx1.xdf", overwrite=TRUE, rowsPerRead = 100000, 
                        rowSelection =SalesTerritory != "Southeast" & CustomerKey != 0 & Profit <= 1000)

rxGetInfo("Cust_data_rx1.xdf")

sales_rx2 <- rxDataStep(sales_rx1, outFile="Cust_data_rx2.xdf",
                        transforms=list(
                            turnover2profit=TotalIncludingTax/Profit
                            ,City = City
                            ,SalesTerritory = SalesTerritory
                            ,TotalIncludingTax = TotalIncludingTax
                            ,Profit = Profit
                        ),
                        overwrite=TRUE, rowsPerRead = 100000)


rxGetInfo("Cust_data_rx2.xdf")
rxGetVarInfo("Cust_data_rx2.xdf")
rxGetVarNames("Cust_data_rx2.xdf")


#sales_rx3 <- rxFactors(sales_rx2,factorInfo="City",outFile="Cust_data_rx3.xdf", overwrite=TRUE) 	


sales_rx4 <- rxSummary(TotalIncludingTax~SalesTerritory:City, data=sales_rx2,
                       summaryStats=c("Mean", "StdDev", "Min", "Max", "Sum"))

sales_rx4_1 <- sales_rx4$categorical[[1]][c("SalesTerritory", "City", "Sum","Max", "Min","StdDev")]

sales_rx4P <- rxSummary(turnover2profit~SalesTerritory:City, data=sales_rx2,
                       summaryStats=c("Mean"))

sales_rx4_2 <- sales_rx4P$categorical[[1]][c("SalesTerritory", "City", "Means")]

## Merge data
sales_rx5 <- merge(sales_rx4_1, sales_rx4_2, by=c("SalesTerritory","City"), all=TRUE)
names(sales_rx5)[3] <- "Total_turnover"
names(sales_rx5)[4] <- "Max_turnover"
names(sales_rx5)[5] <- "Min_turnover"
names(sales_rx5)[6] <- "stdev_turnover"
names(sales_rx5)[7] <- "KPI_avg_Turnover2profit"





help("rxSummary")
