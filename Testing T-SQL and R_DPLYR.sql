/*

** Author: Tomaz Kastrun
** Web: http://tomaztsql.wordpress.com
** Twitter: @tomaz_tsql
** Created: 09.10.2016; Ljubljana
** Comparing execution times using T-SQL and R Package DPLYR 
** to do data manipulation 

*/


USE WideWorldImportersDW;
GO

/*
-- example of using STATISTICS TIME wrapper
SET STATISTICS TIME ON
SELECT GETDATE()
SET STATISTICS TIME OFF
*/



-- ***********************************
--
--  1.  SIMPLE PERFORMANCE TEST
--
-- ***********************************


PRINT ' '
PRINT 'STATISTICS WITH T-SQL'
PRINT ' '

-- TEST Simple T-SQL Query
SET STATISTICS TIME ON
SELECT 
		fs.[City Key]
		,c.City
		,c.[Sales Territory]
		,fs.[Customer Key]
		,fs.[Invoice Date Key]
		,fs.[Quantity]
		,fs.[Unit Price]
		,fs.[Tax Amount]
		,fs.[Total Including Tax]
		,fs.Profit
	FROM [Fact].[Sale] AS  fs
		JOIN dimension.city AS c
		ON c.[City Key] = fs.[City Key]

SET STATISTICS TIME OFF



-- TEST  T-SQL with R

PRINT ' '
PRINT 'STATISTICS WITH R'
PRINT ' '

SET STATISTICS TIME ON

DECLARE @TSQL AS NVARCHAR(MAX)
SET @TSQL = N'SELECT 
		fs.[City Key]
		,c.City
		,c.[Sales Territory]
		,fs.[Customer Key]
		,fs.[Invoice Date Key]
		,fs.[Quantity]
		,fs.[Unit Price]
		,fs.[Tax Amount]
		,fs.[Total Including Tax]
		,fs.Profit
	FROM [Fact].[Sale] AS  fs
		JOIN dimension.city AS c
		ON c.[City Key] = fs.[City Key]'

DECLARE @RScript AS NVARCHAR(MAX)
SET @RScript = N'OutputDataSet <- InputDataSet'

EXEC sys.sp_execute_external_script
		   @language = N'R'
		  ,@script = @RScript
		  ,@input_data_1 = @TSQL
		  
WITH RESULT SETS ((
     [City Key]  INT
	,[City]  NVARCHAR(200)
	,[Sales Territory]  NVARCHAR(200)
	,[Customer Key]  INT
	,[Invoice Date Key]  DATE
	,[Quantity]  INT
	,[Unit Price]  DECIMAL(18,3)
	,[Tax Amount]  DECIMAL(18,3)
	,[Total Including Tax]  DECIMAL(18,2)
	,[Profit]  DECIMAL(18,2)
				 )); 

SET STATISTICS TIME OFF



-- ***********************************
--
--  2.  SIMPLE FILTERING
--
-- ***********************************

PRINT ' '
PRINT 'STATISTICS WITH T-SQL'
PRINT ' '

-- SIMPLE T-SQL
SET STATISTICS TIME ON
SELECT 
		fs.[City Key]
		,c.City
		,c.[Sales Territory]
		,fs.[Customer Key]
		,fs.[Invoice Date Key]
		,fs.[Quantity]
		,fs.[Unit Price]
		,fs.[Tax Amount]
		,fs.[Total Including Tax]
		,fs.Profit
	FROM [Fact].[Sale] AS  fs
		JOIN dimension.city AS c
		ON c.[City Key] = fs.[City Key]
	WHERE
		[Sales Territory] <> 'Southeast'
	AND fs.[Customer Key] <> 0
	AND Profit <= 1000
SET STATISTICS TIME OFF


PRINT ' '
PRINT 'STATISTICS WITH R'
PRINT ' '

-- R Package dplyr and T-SQL
SET STATISTICS TIME ON

DECLARE @TSQL AS NVARCHAR(MAX)
SET @TSQL = N'SELECT 
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
		ON c.[City Key] = fs.[City Key]'

DECLARE @RScript AS NVARCHAR(MAX)
SET @RScript = N'
				library(dplyr)
				OutputDataSet  <- InputDataSet %>% filter(SalesTerritory != "Southeast", CustomerKey != 0, Profit <= 1000)'

EXEC sys.sp_execute_external_script
		   @language = N'R'
		  ,@script = @RScript
		  ,@input_data_1 = @TSQL
		  
WITH RESULT SETS ((
     [City Key]  INT
	,[City]  NVARCHAR(200)
	,[Sales Territory]  NVARCHAR(200)
	,[Customer Key]  INT
	,[Invoice Date Key]  DATETIME
	,[Quantity]  INT
	,[Unit Price]  DECIMAL(18,3)
	,[Tax Amount]  DECIMAL(18,3)
	,[Total Including Tax]  DECIMAL(18,2)
	,[Profit]  DECIMAL(18,2)
				 )); 

SET STATISTICS TIME OFF



-- ***********************************
--
--  2.2  SIMPLE FILTERING
--  dplyr VS. RevoScaleR
--
-- ***********************************



PRINT ' '
PRINT 'STATISTICS WITH R dpylr'
PRINT ' '

SET STATISTICS TIME ON

DECLARE @TSQL AS NVARCHAR(MAX)
SET @TSQL = N'SELECT 
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
		ON c.[City Key] = fs.[City Key]'

DECLARE @RScript AS NVARCHAR(MAX)
SET @RScript = N'
				library(dplyr)
				OutputDataSet  <- InputDataSet %>% filter(SalesTerritory != "Southeast", CustomerKey != 0, Profit <= 1000)'

EXEC sys.sp_execute_external_script
		   @language = N'R'
		  ,@script = @RScript
		  ,@input_data_1 = @TSQL
		  
WITH RESULT SETS ((
     [City Key]  INT
	,[City]  NVARCHAR(200)
	,[Sales Territory]  NVARCHAR(200)
	,[Customer Key]  INT
	,[Invoice Date Key]  DATETIME
	,[Quantity]  INT
	,[Unit Price]  DECIMAL(18,3)
	,[Tax Amount]  DECIMAL(18,3)
	,[Total Including Tax]  DECIMAL(18,2)
	,[Profit]  DECIMAL(18,2)
				 )); 

SET STATISTICS TIME OFF



PRINT ' '
PRINT 'STATISTICS WITH R RevoCcaleR'
PRINT ' '


SET STATISTICS TIME ON

DECLARE @TSQL1 AS NVARCHAR(MAX)
SET @TSQL1 = N'SELECT 
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
		ON c.[City Key] = fs.[City Key]'

DECLARE @RevoRScript AS NVARCHAR(MAX)
SET @RevoRScript = N'
					library(RevoScaleR)
					OutputDataSet <- rxXdfToDataFrame(rxDataStep(inData = InputDataSet, outFile="Cust_data_filter.xdf", overwrite=TRUE, rowsPerRead = 100000, 
					rowSelection =SalesTerritory != "Southeast" & CustomerKey != 0 & Profit <= 1000))'

EXEC sys.sp_execute_external_script
		   @language = N'R'
		  ,@script = @RevoRScript
		  ,@input_data_1 = @TSQL1
		  
WITH RESULT SETS ((
     [City Key]  INT
	,[City]  NVARCHAR(200)
	,[Sales Territory]  NVARCHAR(200)
	,[Customer Key]  INT
	,[Invoice Date Key]  DATETIME
	,[Quantity]  INT
	,[Unit Price]  DECIMAL(18,3)
	,[Tax Amount]  DECIMAL(18,3)
	,[Total Including Tax]  DECIMAL(18,2)
	,[Profit]  DECIMAL(18,2)
				 )); 

SET STATISTICS TIME OFF



-- ***********************************
--
--  3.  AGGREGATE RESULTS
--  TSQL -dplyr VS. RevoScaleR
--
-- ***********************************


PRINT ' '
PRINT 'STATISTICS WITH T-SQL'
PRINT ' '

-- SIMPLE T-SQL
SET STATISTICS TIME ON
SELECT 
		 c.City
		,c.[Sales Territory]
		,SUM(fs.[Total Including Tax]) AS Total_turnover
		,MAX(fs.[Total Including Tax]) AS Max_turnover
		,MIN(fs.[Total Including Tax]) AS Min_turnover
		--,(fs.[Total Including Tax]) AS Median_turnover
		,VAR(fs.[Total Including Tax]) AS Var_turnover
		,STDEV(fs.[Total Including Tax]) AS stdev_turnover
		,AVG(fs.[Total Including Tax]/fs.Profit) AS KPI_avg_Turnover2profit
		
	FROM [Fact].[Sale] AS  fs
		JOIN dimension.city AS c
		ON c.[City Key] = fs.[City Key]
	WHERE
		[Sales Territory] <> 'Southeast'
	AND fs.[Customer Key] <> 0
	AND Profit <= 1000
	GROUP BY
		 c.[Sales Territory]
		,c.City

SET STATISTICS TIME OFF


PRINT ' '
PRINT 'STATISTICS WITH R dpylr'
PRINT ' '

SET STATISTICS TIME ON

-- Difference with T-SQL, I Have to pass all the values needed to filter out and aggregate data
DECLARE @TSQL1 AS NVARCHAR(MAX)
SET @TSQL1 = N'SELECT 
					 c.City
					,c.[Sales Territory] AS SalesTerritory
					,fs.[Customer Key] AS CustomerKey
					,fs.[Total Including Tax] AS TotalIncludingTax
					,fs.Profit
				FROM [Fact].[Sale] AS  fs
					JOIN dimension.city AS c
					ON c.[City Key] = fs.[City Key]'

DECLARE @RdplyrScript AS NVARCHAR(MAX)
SET @RdplyrScript = N'
				library(dplyr)
				OutputDataSet  <- InputDataSet %>% 
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
									  )'

EXEC sys.sp_execute_external_script
		   @language = N'R'
		  ,@script = @RdplyrScript
		  ,@input_data_1 = @TSQL1
		  
WITH RESULT SETS ((
     [City]  NVARCHAR(200)
	,[Sales Territory]  NVARCHAR(200)
	,Total_turnover DECIMAL(18,3)
	,Max_turnover DECIMAL(18,3)
	,Min_turnover DECIMAL(18,3)
	,Median_turnover DECIMAL(18,3)
	,Var_turnover DECIMAL(18,3)
	,stdev_turnover DECIMAL(18,3)
	,KPI_avg_Turnover2profit DECIMAL(18,3)
	)); 

SET STATISTICS TIME OFF




PRINT ' '
PRINT 'STATISTICS WITH R RevoScaleR'
PRINT ' '


SET STATISTICS TIME ON

-- Difference with T-SQL, I Have to pass all the values needed to filter out and aggregate data
DECLARE @TSQL2 AS NVARCHAR(MAX)
SET @TSQL2 = N'SELECT 
					 c.City
					,c.[Sales Territory] AS SalesTerritory
					,fs.[Customer Key] AS CustomerKey
					,fs.[Total Including Tax] AS TotalIncludingTax
					,fs.Profit
				FROM [Fact].[Sale] AS  fs
					JOIN dimension.city AS c
					ON c.[City Key] = fs.[City Key]'

DECLARE @RevoRScript AS NVARCHAR(MAX)
SET @RevoRScript = N'
					library(RevoScaleR)
					sales_rx1 <- rxDataStep(inData = InputDataSet, outFile="Cust_data_rx1.xdf", overwrite=TRUE, rowsPerRead = 100000, 
								  rowSelection =SalesTerritory != "Southeast" & CustomerKey != 0 & Profit <= 1000)

					sales_rx2 <- rxDataStep(sales_rx1, outFile="Cust_data_rx2.xdf",
                        transforms=list(
                            turnover2profit=TotalIncludingTax/Profit
                            ,City = City
                            ,SalesTerritory = SalesTerritory
                            ,TotalIncludingTax = TotalIncludingTax
                            ,Profit = Profit
                        ),
                        overwrite=TRUE, rowsPerRead = 100000)
					
						sales_rx4 <- rxSummary(TotalIncludingTax~SalesTerritory:City, data=sales_rx2,
											   summaryStats=c("Mean", "StdDev", "Min", "Max", "Sum"))

						sales_rx4_1 <- sales_rx4$categorical[[1]][c("SalesTerritory", "City", "Sum", "StdDev", "Min", "Max")]

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
																
					OutputDataSet <- sales_rx5'

EXEC sys.sp_execute_external_script
		   @language = N'R'
		  ,@script = @RevoRScript
		  ,@input_data_1 = @TSQL2
		  
WITH RESULT SETS ((
     [City]  NVARCHAR(200)
	,[Sales Territory]  NVARCHAR(200)
	,Total_turnover DECIMAL(18,3)
	,Max_turnover DECIMAL(18,3)
	,Min_turnover DECIMAL(18,3)
	,stdev_turnover DECIMAL(18,3)
	,KPI_avg_Turnover2profit DECIMAL(18,3)
	)); 

SET STATISTICS TIME OFF







-- ***********************************
--  Additional TEST
--  3.  AGGREGATE RESULTS
--  TSQL  and  dplyrXdf Package
--
-- ***********************************


PRINT ' '
PRINT 'STATISTICS WITH R RevoScaleR using dplyrXdf'
PRINT ' '


SET STATISTICS TIME ON

DECLARE @TSQL2 AS NVARCHAR(MAX)
SET @TSQL2 = N'SELECT 
					 c.City
					,c.[Sales Territory] AS SalesTerritory
					,fs.[Customer Key] AS CustomerKey
					,fs.[Total Including Tax] AS TotalIncludingTax
					,fs.Profit
				FROM [Fact].[Sale] AS  fs
					JOIN dimension.city AS c
					ON c.[City Key] = fs.[City Key]'

DECLARE @RevoRScript AS NVARCHAR(MAX)
SET @RevoRScript = N'
					library(RevoScaleR)
					library(dplyr)
					library(dplyrXdf)
					custXdf <- rxDataFrameToXdf(InputDataSet, outFile="Cust_data_aggregate.xdf", overwrite=TRUE)
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
				OutputDataSet <- data.frame(custagg)'

EXEC sys.sp_execute_external_script
		   @language = N'R'
		  ,@script = @RevoRScript
		  ,@input_data_1 = @TSQL2
		  
WITH RESULT SETS ((
     [City]  NVARCHAR(200)
	,[Sales Territory]  NVARCHAR(200)
	,Total_turnover DECIMAL(18,3)
	,Max_turnover DECIMAL(18,3)
	,Min_turnover DECIMAL(18,3)
	,Median_turnover DECIMAL(18,3)
	,Var_turnover DECIMAL(18,3)
	,stdev_turnover DECIMAL(18,3)
	,KPI_avg_Turnover2profit DECIMAL(18,3)
	)); 

SET STATISTICS TIME OFF

*/