# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #SHOP PERFORMANCE ANALYSIS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Import Library

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Ingestion

# COMMAND ----------

# DBTITLE 1,Ingesting customers
customers=spark.table("shop_performance.analysis.customers")
customers=customers.toPandas()

# COMMAND ----------

# DBTITLE 1,Ingesting orders
orders=spark.table("shop_performance.analysis.orders")
orders=orders.toPandas()

# COMMAND ----------

# DBTITLE 1,Ingesting payments
payments=spark.table("shop_performance.analysis.payments")
payments=payments.toPandas()

# COMMAND ----------

# DBTITLE 1,Ingesting products
products=spark.table("shop_performance.analysis.products")
products=products.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory Data Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Customers Table EDA

# COMMAND ----------

# DBTITLE 1,STEP 1: Get to know the data
# Show the customers table 
display(customers)

# COMMAND ----------

# Show first 5 rows of table 
customers.head()

# COMMAND ----------

# How many rows and columns does customers have?
customers.shape

# COMMAND ----------

# Check summary of information about customers
customers.info()

## Observation:
 # Age is a float (decimal), should be integer

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summary of getting to know the data
# MAGIC - The table has 10,000 rows and 5 columns (CustomerID, Age, City, SignupDate, and CustomerSegment)
# MAGIC - One row represents one customer, therefore table has 10,000 customers
# MAGIC - CustomerID and Age are numbers, 
# MAGIC - City, SignupDate, and CustomerSegment are texts
# MAGIC - Observation: Age datatype is read as a float instead of whole numbers (int64), and signupdate is an object instead of a date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Questions that can be derived from table
# MAGIC - Which city has more customers/ highest number of customers?
# MAGIC - Which segment do most of the customers belong?
# MAGIC - How has the number of customers changed overtime? (based om signup)

# COMMAND ----------

# DBTITLE 1,STEP 2: Clean the data
# Check for missing values - total number of nulls per column
customers.isnull().sum()

# COMMAND ----------

# Check for stastistics summary in numeric columns
customers ["Age"].describe()

# COMMAND ----------

# See the 180 missing Age values
customers[customers["Age"].isna()] 

# COMMAND ----------

# Checking null values and percentage contribution to dataset

# a. Count missing values in the age column
customers["Age"].isnull().sum()

# COMMAND ----------

# b.Calculate the percentage of missing values
customers["Age"].isnull().mean() * 100

## Observations:
 # The missing Age values make up 1.79% of the dataset, less than 5%

# COMMAND ----------

# Check if all non-missing values are whole numbers
customers["Age"].dropna().apply(float.is_integer).all()

# COMMAND ----------

# Converting age as all non-missing Age values are integers and not floats
customers["Age"]=customers["Age"].astype("Int64")

# COMMAND ----------

# Confirming conversion
customers["Age"].dtypes

# COMMAND ----------

# Checking range of customer age
print(customers["Age"].min())
print(customers["Age"].max())
print(customers["Age"].mean())

# COMMAND ----------

# Replace the missing age values with the mean age value - how will this affect analysis?

# COMMAND ----------

# Handling the null age values: Keep  
age_data = customers.dropna(subset=["Age"]) 

## Note:
# I will keep the null age values because the missing values represent a relatively small proportion of the data (1.79%), and I want to analyse the actual recorded ages without introducing estimated values

# COMMAND ----------

# Check number of rows in customers after handling null
print("Original rows:", customers.shape[0])
print("Rows after removing missing ages:", age_data.shape[0])

# COMMAND ----------

# Check missing values before and after cleaning age null values
print("Missing ages before:", customers["Age"].isnull().sum())
print("Missing ages after:", age_data["Age"].isnull().sum())

## Missing ages will not be in analysis

# COMMAND ----------

# See new dataframe with removed missing age 
age_data.head()

# COMMAND ----------

age_data[age_data["Age"].isna()] 

# COMMAND ----------

# See the 119 null cities
customers[customers["City"].isna()] # Ok they are recorded as None, I will not remove the null cities but I'll categorize as 'Unknown' 

# COMMAND ----------

# Checking text consistency in the City column
customers["City"].unique() #I have to standardize 'Tehran'/'tehran' and 'Masshad'/'Mashad' and None to 'Unknown'

# COMMAND ----------

# Standardizing City names capitalization - 'Tehran'/'tehran'
customers["City"] = customers["City"].astype("string").str.strip().str.title()

# COMMAND ----------

# Standardize spelling variations of 'Masshad'
customers["City"] = customers["City"].replace({"Mashhad": "Mashhad", "Mashad": "Mashhad"})

# COMMAND ----------

# Replacing null City values
customers["City"]=customers["City"].replace(
    ["None"], ["Unknown"]
).fillna("Unknown")

display(pd.DataFrame(customers["City"].unique(), columns=["City"]))

# COMMAND ----------

customers["City"].value_counts()

# COMMAND ----------

# See signupdate data type
customers.dtypes

# COMMAND ----------

# Convert SignupDate column data type to date
customers["SignupDate"]=pd.to_datetime(customers["SignupDate"])

# COMMAND ----------

# Check conversion of SignupDate to datetime from object
customers.dtypes

# COMMAND ----------

# Check SignupDate range

# a. First SignupDate 
customers["SignupDate"].min()

## First customer SignupDate is 2023-01-01

# COMMAND ----------

# b. Last SignupDate
customers["SignupDate"].max()

## Last customer SignupDate is 2025-12-30

# COMMAND ----------

# Check for total duplicated rows: are any customers recorded more than once 
customers.duplicated().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary of Problems identified
# MAGIC - The Age and City columns have missing values. 'Age' has 180 missing values stored as 'NaN', and 'City' has 119 missing values stored as 'None'
# MAGIC - As a result, the 'Age' column was interpreted as a float64, instead of an Int64
# MAGIC - 'City' also has text inconsitency issues such as capitalization and spelling variations. This is identified on two city names, Tehran and Mashhad.
# MAGIC - The 'SignupDate' is interpreted as an object data type instead of a date

# COMMAND ----------

# MAGIC %md
# MAGIC #### What I decided to do: 
# MAGIC - I first checked if all non-missing 'Age' values are whole numbers or decimals using, customers["Age"].dropna().apply(float.is_integer).all(), and it returned 'True', this means that all non-missing ages are whole numbers, so there isn't really a fractional aged customer,
# MAGIC - I then converted the data type to an integer using, customers["Age"]=customers["Age"].astype("Int64"), at the confirmation so that I can use whole numbers when doing any age related analysis such as finding the range
# MAGIC - In the 'City' column, I applied replacement codes on the individual inconsitencies found;
# MAGIC > - customers["City"] = customers["City"].astype("string").str.strip().str.title() - to change capitalization issue of 'Tehran' and 'tehran', and return it as one Tehran 
# MAGIC > - customers["City"] = customers["City"].replace({"Mashhad": "Mashhad", "Masshad": "Mashhad"}) - to standardize spelling variations of Mashhad, as there were two spellings that were counted twice also,
# MAGIC > - customers["City"]=customers["City"].replace(["None"], ["Unknown"]).fillna("Unknown") - .fillna helped to replace the actual missing value 'None' instead of replacing the string by a null
# MAGIC - For the 'SignupDate' column, I converted the date type from an object to a datetime to help me with further time analysis such as customer base change overtime

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Orders Table EDA

# COMMAND ----------

# DBTITLE 1,STEP 1: Get to know the data
# Show the orders table 
display(orders)

# COMMAND ----------

# Show last 5 rows
orders.tail()

# COMMAND ----------

# How many rows and columns does each file have?
orders.shape

# COMMAND ----------

# Check summary information about orders
orders.info()

## Observations:
 # Columns with missing values: OrderDate, Quantity, Discount, PaymentMethod
 # OrderDate is an object instead of a date = conversion
 # Also, Quantity is a float instead of an integer = conversion


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summary of getting to know the data
# MAGIC - The table has 50,120 rows and 8 columns (OrderID, CustomerID, OrderDate, ProductID, Quantity, Discount, PaymentMethod, Status)
# MAGIC - One row represents one order, therefore table has 50,120 orders
# MAGIC - OrderID, CustomerID, ProductID, Quantity, and Discount are numbers,
# MAGIC -  OrderDate, PaymentMethod, Status are texts
# MAGIC - Observation: OrderDate datatype is read as an object instead of a date
# MAGIC - Columns with missing values: OrderDate, Quantity, Discount, PaymentMethod

# COMMAND ----------

# MAGIC %md
# MAGIC #### Questions that can be derived from table
# MAGIC - How many orders were made in 2024 vs 2025?
# MAGIC - How many orders were completed/returned/cancelled? (What is the distribution of orders by status)
# MAGIC - How does order quantity vary over time?
# MAGIC - Which cities generate the highest number of orders? ( join with customers table )

# COMMAND ----------

# DBTITLE 1,STEP 2: Clean the data
# Check for missing values - total number of nulls per column
orders.isnull().sum()

# COMMAND ----------

# See summary statistics of OrderDate
orders["OrderDate"].describe()

## Observations:
 # Confirmation of 35 missing date values - non-missing values is 50085 out of 50120
 # Starting date is 2024-01-01 to 2026-06-30
 # this is two (2) full years plus half year order records
 # data type is object instead of date 

# COMMAND ----------

# See missing 35 OrderDate values - filter
orders[orders["OrderDate"].isna()] 

## Observations:
 # Most Orders have a Completed status
 # null values are stringed as None, why were dates not recorded when orders were completed?



# COMMAND ----------

# Check if missing dates has anything common with the other columns:

# a. Status - Check how many of the OrderDate missing values are 'Cancelled', 'Returned', or 'Completed'
orders[orders["OrderDate"].isna()]["Status"].value_counts() 

## Observations:
 # 33 Orders have a Completed status
 # 1 is Returned and 1 is Cancelled
 # Probably a case of data recording issues more than unsuccessful orders

# COMMAND ----------


# b. Payment Methods 
orders[orders["OrderDate"].isna()]["PaymentMethod"].value_counts(dropna=False) 

## Observation:
 # 20 out of 35 missing date orders used Gateway,
 # 12 are shared between CardToCard and Wallet payment,
 # and 3 used cash

# COMMAND ----------

# c. ProductID - products occuring amongst missing dates
orders[orders["OrderDate"].isna()]["ProductID"].value_counts()

## Observation:
 # ProductID 2013 and 2004 have the highest count of missing date orders association
 # This is 15/35 = 42.3% concentration 

# COMMAND ----------

# d. CustomerID - how many unique customers are involved
orders[orders["OrderDate"].isna()]["CustomerID"].nunique() 

## Observations:
 # All 35 missing dates came from 35 unique customers

# COMMAND ----------

# Comparing the missing date to the entire orders table
missing_date_pct = (orders["OrderDate"].isna().mean()*100).round(2)

print(missing_date_pct) 

## Observations:
 # Missing OrderDate values account for 0.07% of all orders, this indicates that the missingness affects a very small proportion of the dataset
 # I will therefore exclude these records from analysis requiring a valid order date, but will not remove them or fill them

# COMMAND ----------

# Check data type of OrderDate to see if values had an impact on the interpretation - parse all non-null values as dates
orders["OrderDate"].describe() # object dtype, 50085 counted, 35 nulls

pd.to_datetime(orders["OrderDate"].dropna(), errors="coerce").notna().all()

# COMMAND ----------

# Converting OrderDate to datetime from object
orders["OrderDate"] = pd.to_datetime(orders["OrderDate"])

# COMMAND ----------

# Confirming conversion of OrderDate
orders.dtypes

# COMMAND ----------

# Check OrderDate range 
orders["OrderDate"].min() 

## Order date spans from 2024


# COMMAND ----------

orders["OrderDate"].max() 

## Order date extends to June 2026
 # This means that 2026 has a partial year (half year) orders record, while 2024 and 2025 have full year records

# COMMAND ----------

# Yearly order volume: Verifying date range too
orders["OrderDate"].dt.to_period("Y").value_counts().sort_index() 

## Observations:
 # Verifying order dates coverage, confirming 35 missing dates and date range
 # Orders span between 2024 - 2026
 # 2026 has a smaller order date volume due to partial year
 # Sum of the value counts = 50085, when subtracted from the total orders rows (50120, we are left with 35 null or non-date OrderDate values



# COMMAND ----------

# Checking Monthly order volume
orders["OrderDate"].dt.to_period("M").value_counts().sort_index()

# COMMAND ----------

# See the summary statistics of numerical columns
orders.describe()

# COMMAND ----------

# See the  null quantities
orders[orders["Quantity"].isna()]

# COMMAND ----------

# See summary statistics of Quantity
orders["Quantity"].describe() 

## Observations:
 # Confirming 80 missing values - 50040 non-missing values are counted out of 50120
 # The minimum Quantity of orders made is -2,
 # a negative quantity does not make sense because units ordered can't be negative, either 0 or >0,
 # so, what do the negative values represent? Errors/returns/refunds/corrections?
 # The maximum Quantity is 5
 # Data type is a float, can we have a fractional order? 
 # change to integer 

# COMMAND ----------

# Check whether all non-missing quantities are actually whole numbers and not floats
orders["Quantity"].dropna().apply (float.is_integer).all()

# COMMAND ----------

# Convert Quantity from float64 to Int64 as non-missing values are confirmed as integers
orders["Quantity"] = orders["Quantity"].astype("Int64")

# COMMAND ----------

# Verify my Quantity conversion dtype
orders["Quantity"].dtypes

# COMMAND ----------

# See how many unit quantities are recorded from positive to negative values
orders["Quantity"].value_counts() 

## Obsrvations:
 # 19 unit quantities are negative; check the commonalities with other columns to see if it is an error or useful business data

# COMMAND ----------

# Verifying Quantity values that are negative
orders[orders["Quantity"]<0]["Status"].value_counts() 


# COMMAND ----------

# See association of missing quantities with status
# Check how many of the missing values are 'Cancelled', 'Returned', or 'Completed'
orders[orders["Quantity"].isna()]["Status"].value_counts() 

## Observations:
 # Confirmation of 80 missing quantity values 
 # 72 are completed orders
 # 7 are cancelled orders
 # Only 1 is a returned order
 # Therefore, most missing quantity values are associated with completed orders, this means that missing quantity values are not limited to cancelled or returned orders

# COMMAND ----------

# See  summary stats for negative quantity values only
orders[orders["Quantity"]<0]["Quantity"].describe()

# COMMAND ----------

# See association of negative values with status
orders[orders["Quantity"]<0]["Status"].value_counts() 

## Observations:'
 # 18 out of 19 negative quantity values are Completed orders 
 # Only 1 is Cancelled 
 # None of the 19 negative quantity values have a Returned status, that is, none of the values less than 0 are associated with Returned orders
 # Also, the relationship beteween negative quantities and order status requires further investigation before deciding whether these values represent data errors or a valid business process 

# COMMAND ----------

# Show unit quantities that are negative 
orders[orders["Quantity"]<0]

# COMMAND ----------

### See the discount patterns
orders["Discount"].describe()

# COMMAND ----------

# Check the unique values of Discount
orders["Discount"].unique() 

## Observations:
 # 0.00 format represents percentage discount
 # Find association of NaN with other columns e.g. Payment method or product ID/Name (from products.csv)

# COMMAND ----------

# See null values in Discount 
orders[orders["Discount"].isnull()] 

## Observations:
 # Confirming null (NaN)values =221 out of 50120 rows 

# COMMAND ----------

# See association of null values with other columns
orders[orders["Discount"].isna()]["PaymentMethod"].value_counts()

## Observation:
 # Many null discount values are amongst the Gateway payment method (91), 
 # Gateway payment method seems to be the common column for the missing values probably because it may be the most frequently used payment method
 # Gateway is followed by CardToCard payment method at 56, closely followed by Wallet payment at 49, and the least counts of missing discount values is associated with Cash payment at 22
 # There is a vast difference between Gatway and the other methods =outlier

# COMMAND ----------

# Establishing whether Gateway is disproporrionally affected using missing discount values
orders.groupby("PaymentMethod")["Discount"].apply(lambda x: x.isna().mean()*100)

# COMMAND ----------

# Checking distribution of discounts
orders["Discount"].value_counts(dropna=False).sort_index() 

## Observations:
 # The dataset uses discounts ranging from 0% to 30%, with 0% being the most applied discount at 17435 out of 50120, follwed by 5% (10888) and 10% (10009)

# COMMAND ----------

orders[orders["Discount"].isna()]["ProductID"].value_counts()

# COMMAND ----------

# Check Payment method - how customeers are paying
orders["PaymentMethod"].value_counts()

## Observations:
 # The most frequenlty used payment method is confirmed to be  Gateway payment method, 23877 out of 50120
 # The second payment method is CardToCard method at 11981, followed by Wallet (8804) and lastly Cash payment (5006)
 # This could explain why Gateway payment method was found to be mostly frequently common value when associated with the missing values of other columns

# COMMAND ----------

# Verifying the missing PaymentMethod values
orders["PaymentMethod"].isna().sum()

## Notes:
 # 452 out of 50120 orders = 0.90% 
 # This is a relatively small portion of the dataset actually,
 # So, 0.90% orders have missing payment-method information, this means that 99.1% orders have information

# COMMAND ----------

# Check association of PaymentMethods with other columns

#a. Status - which statuses have missing payment methods
orders[orders["PaymentMethod"].isna()]["Status"].value_counts() 

## Observation:
 # Missing PaymentMethod values are concetrated in Completed orders
 # This could mean that mising values are not simply because orders were cancelled or returned 

# COMMAND ----------

# Show missing payment methods 
orders[orders["PaymentMethod"].isnull()]

# COMMAND ----------

# b. Discounts - how many orders have both PaymentMethod and Discount missing
orders[orders["PaymentMethod"].isna()]["Discount"].isna().sum()

## Observations:
 # 3 order records are missing both PaymentMethod and Discount

# COMMAND ----------

# c. Quantity - how many orders have both PaymentMethod and Quantity missing
orders[orders["PaymentMethod"].isna()]["Quantity"].isna().sum()

# COMMAND ----------

# Check actual payment methods 
orders["PaymentMethod"].value_counts(dropna=False)

# COMMAND ----------

# Checking for duplicated records
orders.duplicated().sum()

## Observations:
 # 120 rows out of 50120 rows are duplicated, this is 0.24%  of the table - small proportion but may affect the analysis

# COMMAND ----------

# See ALL duplicated records - original and copy appearance
orders[orders.duplicated(keep=False)].sort_values("OrderID") 

## Observations:
 # Duplicates happen in OrderID and duplicates are exactly the same/ the entire row is repeated
 # 240 rows duplicate confirms 120 duplicates
 # I will remove duplicates

# COMMAND ----------

# Remove duplicates
orders = orders.drop_duplicates()

# COMMAND ----------

# Verify removal of duplicates
orders.duplicated().sum()
 
 # Duplicates removed

# COMMAND ----------

# Check newrow count 
orders.shape

# COMMAND ----------

# Display cleaned table
display(orders)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary of Problems identified
# MAGIC - Four columns have missing values. 'OrderDate' has 35 missing values stored as 'None', 'Quantity' has 80 missing values stored as 'NaN', 'Discount' has 221 missing values stored as 'NaN', and 'PaymentMethod' has 452 missing values stored as 'None',
# MAGIC - OrderDate datatype was given as object instead of datetime
# MAGIC - Quantity datatype was given as a float instead of an integer, and it also has 19 negative values
# MAGIC - 120 exact duplicate records were identified

# COMMAND ----------

# MAGIC %md
# MAGIC #### What I decided to do: 
# MAGIC - I changed the datatype of OrderDate to datetime after I checked if all non-missing date values are dates using pd.to_datetime(orders["OrderDate"].dropna(), errors="coerce").notna().all(), and it returned 'True', this means that all non-mising OrderDates are dates, so the missing values influenced the data type
# MAGIC - I also changed the datatype of Quantity using a similar logic 
# MAGIC - The 120 duplicate records were removed to prevent double counting in subsequent analysis
# MAGIC
# MAGIC - Payment Method:
# MAGIC > - 452 values are missing orders i.e. 0.90%
# MAGIC > - Missing payment methods were examined against order status, discount and quantity variables to identify pontential relation or pattern
# MAGIC > - Missing values were not replaced because the actual payment method cannot be reliably determined or confirmed from the available data

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Payments Table EDA

# COMMAND ----------

# DBTITLE 1,STEP 1: Get to know the data
# Show the payments table 
display(payments)

# COMMAND ----------

# Show first 5 rows of table 
payments.head()

# COMMAND ----------

# How many rows and columns does the table have
payments.shape

## Observations:
 # The table has 50,000 rows and 4 columns
 # Columns are; PaymentID, OrderID, PaymentDate, and PaymentStatus 

# COMMAND ----------

# Check summary of information about payments
payments.info()

## Observations:
 # PaymentDate has missing values and it is interpreted as an object instead of a date
 # All other variables/columns in the table do not have missing values 

# COMMAND ----------

# See statistical summary of table
payments.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summary of getting to know the data
# MAGIC - The table has 50,000 rows and 4 columns (PaymentID, OrderID, PaymentDate, PaymentStatus)
# MAGIC - One row represents one payment attempt, therefore table has 50,000 payment records
# MAGIC - PaymentID and OrderID are numbers (int64), 
# MAGIC - Observation: PaymentDate is interpreted as an object/text and,
# MAGIC - PaymentStatus is a text
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Questions that can be derived from table
# MAGIC - What proportion of paymemnts are successful/paid?
# MAGIC - How many payments failed or were refunded or pending?
# MAGIC - Are there multiple payment attempts for some orders?
# MAGIC - Are payment dates missing or outside the expected period?
# MAGIC - Do payment status align with order status?
# MAGIC

# COMMAND ----------

# DBTITLE 1,STEP 2: Clean the data
# Check for missing values
payments.isna().sum()

## Observation: 
 # Only PaymentDate has missing values 

# COMMAND ----------

# a.See the PaymentDate missing values
payments[payments["PaymentDate"].isna()]

## Observations:
 # Payment status is None for all missing status values

# COMMAND ----------

# Comparing missing payment dates with payment status - Checking payment statuses where payment date is missing
payments[payments["PaymentDate"].isna()]["PaymentStatus"].value_counts() 

# COMMAND ----------

# Create a cross-tabulation for the comparison between date and status
pd.crosstab(payments["PaymentStatus"], payments["PaymentDate"].isna(),margins=True)

## Observations:
 # Missing dates are true or found only among Paid payment status, and not the Failed or Refunded payments
 # This could indicate that there was a potential data recording issue as the missing dates are associated with successful payment status
 # I will investigate the affected records against the corresponding order dates before any data cleaning decision such as removing or replacing by connecting thesemissing values to the orders table

# COMMAND ----------

# Confirm PaymentDate data type
payments["PaymentDate"].dtype

## Data type is object

# COMMAND ----------

# Converting PaymentDate dtype from object to datetime
payments["PaymentDate"] = pd.to_datetime(payments["PaymentDate"], errors="coerce")

# COMMAND ----------

# Check date range 

# a. First date
payments["PaymentDate"].min()

## First date = 2024-01-01

# COMMAND ----------

# b. Last date
payments["PaymentDate"].max()

## Last date = 2026-06-30
 # Payments date range corresponds with orders date range, both start on 2024-01-01 and end on 2026-06-30 = two years and 6 months

# COMMAND ----------

# Check for duplicates 
payments.duplicated().sum()

## Observations:
 # There are no duplicated rows 

# COMMAND ----------

# Check if payment id is unique
payments["PaymentID"].duplicated().sum()

## PaymentID is not duplicated 

# COMMAND ----------

# Checking unique values - PaymentStatus
payments["PaymentStatus"].unique()

# COMMAND ----------

# Checking incosistencies in categorical/text data - PaymentStatus
payments["PaymentStatus"].value_counts()

## Observations:
 # There are 3 categories for payment status, no inconsistenccy in spellings
 # 46,569 payments out of 50,000 were successful (Paid), this is equivalent to 93.14% of the payments that are completed
 # 1924 payments out of 50,000 were unsuccessful (Failed), this is equivalent to 3.84% of the payments - there could be potential payment problems
 # 1507 payments out of 50,000 were Refunded to customers, this is equivalent to 3.01% of the payments
 # Failed and Refunded payments make up 6.85% of unsuccessful payments


# COMMAND ----------

# Check for possible multiple payments per order - payment retries or seperate payment attempts
payments["OrderID"].value_counts().value_counts().sort_index()

## Observations:
 # No multiple payment records were identified per OrderID,
 # this means there is no evidence of multiple payment attempts or payment records/retries per order in this table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Connecting payments to orders

# COMMAND ----------

# Check whether payment records match or exist in the orders table
payments["OrderID"].isin(orders["OrderID"]).value_counts()

## Observations:
 # All 50,000 payment records have a matching OrderID in the orders table,
 # Each OrderID appears only once in the payments table,
 # Therefore, no unmatched payment records and each order has one payment record

# COMMAND ----------

# Confirming if every order also has a matching payment record since they both have 50,000 rows
orders["OrderID"].isin(payments["OrderID"]).value_counts()

## Observations:
 # I have confirmed that every order also has a matching payment record

# COMMAND ----------

# Check missing payment dates against order dates
missing_dates =  payments[payments["PaymentDate"].isna()]

missing_dates.merge(orders[["OrderID", "OrderDate"]], on="OrderID", how="left")

## Observations:
 # PaymentDate from payments table and OrderDate from orders table both have missing value 

# COMMAND ----------

# Further comparison

# Select payments with missing payment dates
missing_payment_dates =  payments[payments["PaymentDate"].isna()].copy()

# COMMAND ----------

# Connect them to orders table
missing_payment_dates = missing_payment_dates.merge(orders[["OrderID", "OrderDate"]], on="OrderID", how="left", indicator=True)

# COMMAND ----------

# Check the order dates and matching records 
pd.crosstab(missing_payment_dates["PaymentStatus"], missing_payment_dates["OrderDate"].isna(), margins=True)

## Observations:
 # Both dates are missing 

# COMMAND ----------

# Check whether the 35 records matched an order
missing_payment_dates["_merge"].value_counts()

## Observation:
 # all 35 payment match order

# COMMAND ----------

# Count missing order dates in the original orders table
orders["OrderDate"].isna().sum()

## Missing dates from orders table is independently 35 also
 # All 35 records with missing PaymentDate values have a Paid status and also have missing OrderDate values in the joined data 
 # this suggests that the missing dates may be related across the two tables 
 # The records are retained for further investigation
 # These missing records cannot currently be used for analysis that require a valid payment date or order date, however, their payment status can still be used in status-based analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary of Problems identified
# MAGIC - Out of the four columns in the payments table, only one had missing values, that is the PaymentDate,
# MAGIC - I has 35 missing values, and these missing values all have a successful payment status
# MAGIC - PaymentDate datatype was given as object instead of datetime due to the nulls
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### What I decided to do: 
# MAGIC - I changed the datatype of PaymentDate to datetime after I checked if all non-missing date values are dates using pd.to_datetime(orders["PaymentDate"].dropna(), errors="coerce").notna().all(), and it returned 'True', this means that all non-mising PaymenrDates are dates, so the missing values influenced the data type
# MAGIC - For the missing values, I first created a cross-tabulation of comparison between the payment date and the payment status using
# MAGIC pd.crosstab(payments["PaymentStatus"], payments["PaymentDate"].isna(),margins=True), to see the relationshhip between paymnet and status,
# MAGIC > - I found out that all 35 missing values had a Paid payment status
# MAGIC - I then proceeded to conduct an investigation by coonecting the PaymentDate missing values with the orders table to see correspondence before deciding to keep, remove or replace the missing values:
# MAGIC > - Correspondence 1 - I began with confirming the missing values of the order dates in the original orders table and found that the missing values are also 35 just like the missing payment date
# MAGIC > -  Correspondence 2 - Payment records matched or existed in the orders table by commomn column OrderID, all 50,000 payment records matched with the 50,000 orders record on OrderID, I used the .isin and .merge function between the two tables
# MAGIC > - Correspondence 3 - All 35 records with missing PaymentDate values have a Paid status and also have missing OrderDate values in the joined data 
# MAGIC - This suggests that the missing dates may be related across the two tables 
# MAGIC - The records are retained for further investigation
# MAGIC - These missing records cannot currently be used for analysis that require a valid payment date or order date, however, their payment status can still be used in status-based analysis
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Products Table EDA

# COMMAND ----------

# DBTITLE 1,STEP 1: Get to know the data
# Show the products table 
display(products)

# COMMAND ----------

# How many rows and columns does products have?
products.shape

## Observation:
 # Products table has 20 rows and 4 columuns (ProductID, ProductName, Category, UnitPrice)
 # UnitPrice will allow us to calculate the revenue

# COMMAND ----------

# See first five rows
products.head()

# COMMAND ----------

# See summary inforamtion about the table 
products.info()

## Observations:
 # The products table has no null values,
 # The data types are appropriate for the variables

# COMMAND ----------

# See the summary statistics of products 
products.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summary of getting to know the data
# MAGIC - The table has 20 rows and 4 columns (ProductID, ProductName, Category, UnitPrice)
# MAGIC - Each row represents one product thatthe shop sells, its name, category and price,
# MAGIC - Therefore one row = 1 product, then products table has 20 different products identified by their unique product id
# MAGIC - ProductID and UnitPrice are numerical data/ numbers
# MAGIC - ProductName and Category are objects/ texts
# MAGIC - There is no missing value
# MAGIC - All data types are appropriate

# COMMAND ----------

# MAGIC %md
# MAGIC #### Questions that can be derived from table
# MAGIC - Which product categories and products contribute most to the shop's order activity?

# COMMAND ----------

# DBTITLE 1,STEP 2: Clean the data
# Confirming no missing values
products.isna().sum() 

# COMMAND ----------

# Check for duplicates
products["ProductID"].duplicated().sum() 

## Observation:
 # There are no duolicated product records

# COMMAND ----------

# Checking unique id to see correspondence with number of records = 20 records
products["ProductID"].nunique()

## Observations:
 # unique id corresponds with number of records, both 20, confirming no dulicate row

# COMMAND ----------

# Check ProductName

#a. Checking different values in product name and observe for inconsistencies
products["ProductName"].unique()

## Observations:
 # There are no incosistencies on spelling or repetitions of text 

# COMMAND ----------

#b. Check how many times each product name value appears in the products table
products["ProductName"].value_counts()

##Observation: 
 # Each product name appears once, and there are 20 values

# COMMAND ----------

# Check Category

# a. See differnt category
products["Category"].unique()

## Observations:
 # There are 5 unique categories
 # There are no inconsistencies in texts, therefore no standardizing needed

# COMMAND ----------

# b. How many times does each category appear in the products table
products["Category"].value_counts()

## Observations:
 # Electronics has the most occurence in the records with 9/20,
 # Followed by Accessories with 5/20,
 # Wearables and Home Office take third place both appearing 2/20,
 # Lastly Stationery and Gaming making 1 out of 20 appearance in the products record
 # This may help identify the category that contributes the most/least to sales


# COMMAND ----------

# Check Unit price range and unusal pricing

# a. Check range of unit price
print(products["UnitPrice"].min())


# COMMAND ----------

print(products["UnitPrice"].max())

# COMMAND ----------

# b. Check for unsual pricing
products[products["UnitPrice"] <= 0] 

## Observations:
 # no unit price is equal to or less than zero


# COMMAND ----------

products[products["UnitPrice"]>260]

## Observations:
 # no unit price is greater than the max price 260, therefore, no extremely high prices

# COMMAND ----------

# Check if every product appearing in Orders actually exists in Products by ProductID
orders["ProductID"].isin(products["ProductID"]).value_counts()

## Observations:
 # All products in the products table appear in the orders table by common column ProductID

# COMMAND ----------

# Is there a big difference between categories statistics -
products.groupby("Category")["UnitPrice"].agg(
    ["count", "mean", "min", "max"]
)

## Observations:
 # Different categories have different price profiles, which needs to be considered when comparing their revenue performance
  # Electronics has the highest max pricing at 260, appearing 9 times, with an average of 73.55 rands in pricing
  # Home Office has second highest max pricing at 180, but appears two times in the products record and has the highest average pricing of 106
  # Accessories, although with second highest appearance in the products records with 5 counts, its average is low at 20.80
 #For example, a category selling fewer units could still generate substantial revenue because its products have higher prices.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary of Problems identified
# MAGIC - Overall, no problems were identified in this dataset that would need cleaning for analysis.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### What I decided to do
# MAGIC - Due to no issues identfied, I will continue to my step 4: Joining the tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytical Data:Sales Performance Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 3: Combining the tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### What I am doing in the next lines of code
# MAGIC - I will check the orders table using a shape function, this will help me refer to the number of rows and check if row count still makes sense after each join,
# MAGIC - I will use orders mainly because it is the base table with primary key identifiers and the shop performance analysis is mainly about orders or sales performance
# MAGIC - I will join tables by common identifier 
# MAGIC > - products to orders through ProductID = I will call the new table sales, and use it going foward to join other tables,
# MAGIC > - customers to sales through CustomerID
# MAGIC > - payments to sales through OrderID
# MAGIC - As a result, I will have three (3) joins, they will all be left joined to Order/sales as the main/base table
# MAGIC
# MAGIC

# COMMAND ----------

# Check orders shape
orders.shape

## 50,000 rows are expected from joins 

# COMMAND ----------

# DBTITLE 1,JOIN 1: Products - Orders
# Joining products to orders through productid
sales = orders.merge(
    products,
    on="ProductID",
    how="left"
)

# COMMAND ----------

# Check row count after join
sales.shape

## Observations:
# Rows are still 50,000 in the join, columns are now 11
# Column count has 3 addtitional columns, must be from products - UnitPrice, ProductName, Category


# COMMAND ----------

# See new table, sales
display(sales)

## UnitPrice, ProductName, and Category are added to the original 8 to 11

# COMMAND ----------

# Check missing values in sales
sales["ProductID"].isna().sum()

# COMMAND ----------

# DBTITLE 1,JOIN 2: Customers-Sales
# Join customers to sales
sales = sales.merge(
    customers[["CustomerID", "City", "CustomerSegment"]],
    on="CustomerID",
    how="left"
)

# COMMAND ----------

display(customers)

# COMMAND ----------

# Check row and colun count after merge
sales.shape

## Observations:
 # Row count still 50,000 and columns are now 13
 # Column count has two additional columns, City and CustomerSegment

# COMMAND ----------

# See new join table - sales + customers
sales.head()

## City and CustomerSegments are added to sales 11 columns to 13

# COMMAND ----------

# Check null values in customers-sales join
sales["CustomerID"].isna().sum()

## Observations:
 # no missing values from join

# COMMAND ----------

# DBTITLE 1,Join 3: payments-sales
# Joining payments to sales
sales = sales.merge(
    payments[["OrderID", "PaymentStatus"]],
    on="OrderID",
    how="left"
)

# COMMAND ----------

# Check row count after join
sales.shape

## Observations:
 # Row count is still maintained at 50,000 and columns increased to 14
 # Column added is PaymentStatus from payments

# COMMAND ----------

# See new columns from payments-sales join
sales.head()

## PaymentStatus from payments has been added resulting in 14 columns

# COMMAND ----------

# Check for nulls on final join table, payments- sales 
sales["PaymentStatus"].isna().sum()

## No missing values in new join, payment status column

# COMMAND ----------

# DBTITLE 1,Joins Duplicate check
# Check the number of unique orders in sales
sales["OrderID"].nunique()

## OrderID does not repeat

# COMMAND ----------

# Check for completely duplicated rows
sales.duplicated().sum()

## No duplicated rows

# COMMAND ----------

display(sales)

# COMMAND ----------

sales.info()

# COMMAND ----------

# Check null values in the joined sales table
sales.isna().sum()

## Observations:
 # OrderDate has 35 out of 50,000 missing values = 0.07%
 # Quantity has 80 out of 50, 000 missing values = 0.16%
 # Discount has 200 out of 50,000 missing values = 0.4%
 # PaymentMethod 450 out of 50,000 missing values = 0,9%
 # City and CustomerSegment from Customers table both have 30 out of 50,000 missing values = 0.06% each
# The sum of mmissing values fron the sales table is 1.65%, this is a relatively small percentage of the dataset. I will handle the missing values from the joined table by creating a cleaned table called sales_clean

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary of information that can be answered by each join
# MAGIC - Join 1 (products and orders = sales)
# MAGIC > - Which products generate the most revenue?
# MAGIC > - Which categories generate the most revenue?
# MAGIC > - Which products sell the most units?
# MAGIC
# MAGIC - Join 2 (customers and sales)
# MAGIC > - Which cities are most valuable?
# MAGIC > - Which customer segments are most valuable?
# MAGIC > - How does revenue differ between New, Regular and VIP customers?
# MAGIC
# MAGIC - Join 3 (payments and sales)
# MAGIC > - What percentage of payments failed?
# MAGIC > - What percentage of orders were cancelled/returned?
# MAGIC > - Do payment issues relate to the sales performance?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary observation on joins
# MAGIC - ProductID values successfully match the products table,
# MAGIC - CustomerID values successfully match the customers table,
# MAGIC - OrderID values successfully match payments table
# MAGIC - Joins have not multiplied/ increased the number of rows

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4: Create new columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### What I will do in this section
# MAGIC - Now that my columns are joined as sales table, I will create new columns in sales that will include useful numbers that are not in the data yet.
# MAGIC These columns are:
# MAGIC > - **Revenue** - for each order, I will calculate revenue using formula: Quantity x UnitPrice x (1 - Discount)
# MAGIC > - **Year** and **Month** - from OrderDate, I will extract the year and the month as Orderdate has been converted to datetime
# MAGIC - I will also verify these new columns and decide if cancelled, returned or unpaid orders count as revenue

# COMMAND ----------

# Creating Revenue column in sales using calculation
sales["Revenue"] = (
    sales["Quantity"] * sales["UnitPrice"] * (1 - sales["Discount"]).round(2)
)

# COMMAND ----------

# Show new column in sales
display(sales)

# COMMAND ----------

# Confirm OrderDate data type For Extracting Month and Year
sales["OrderDate"].describe

## OrderDate is datetime 64

# COMMAND ----------

# a. Extracting year from sales 
sales["Year"] = sales["OrderDate"].dt.year


# COMMAND ----------

# b. Extracting month for sales trend
sales["Month"] = sales["OrderDate"].dt.month

# COMMAND ----------

# Verifying all new columns: Revenue, Year, Month
sales[["Quantity", "UnitPrice", "Discount", "Revenue", "Year", "Month"]].head()

# COMMAND ----------

# Check rows where Revenue is missing
sales[sales["Revenue"].isna()][["OrderID", "ProductID","Quantity", "UnitPrice", "Discount", "Revenue"]]

# COMMAND ----------

# Check the combinations of missing values 
sales[["Quantity", "Discount", "Revenue"]].isna().value_counts()

## Observations:
 # All 300 missing revenue values are associated with either a missing quantity or discount,
 # There are no records where both Quantity and Discount are False (not missing/present) but Revenue is True (missing),
 # This means that if on the null values, missing either Quantity or Discount means that we cannot have Revenue = Need to clean the missing values 
 # Revenue calculation is working therefore

# COMMAND ----------

# Checking the 80 missing Quantity values

# a. See orders with missing quantites
sales.loc[sales["Quantity"].isna(),["OrderID", "ProductID", "OrderDate", "Status", "PaymentStatus", "UnitPrice", "Discount"]].head(25)

# COMMAND ----------

 # Checking the status of records with the 80 missing quantities
sales.loc[sales["Quantity"].isna(), ["Status","PaymentStatus"]].value_counts()

## Observations:
 # Completed and Paid = 71/80 records
 # Cancelled and Paid = 7/80 records
 # Completed and Refunded = 1/80 records
 # Returned and Paid = 1/80 records

# COMMAND ----------

# Confirming missing values in OrderDate
sales["OrderDate"].isna().sum()

# COMMAND ----------

# KEEP See affected orders and their statuses
sales[sales["OrderDate"].isna()][["OrderID", "OrderDate","Status", "PaymentStatus"]].head(15) 

## Observations on joining orders and payment revealed that all 35 undated orders were successfully paid 

# COMMAND ----------

# Missing OrderDates - Check status of orders with missing dates
sales.loc[sales["OrderDate"].isna(), ["OrderID", "OrderDate", "Status", "PaymentStatus"]]["Status"].value_counts()

# COMMAND ----------

# b. Check rows, columns and duplicated records
print("Shape:", sales.shape)
print("Duplicate rows:", sales.duplicated().sum())

## No duplicated rows, I just have to handle the missing dates and revenues in the new sales table and recreate them for analysis

# COMMAND ----------

display(sales)

# COMMAND ----------

print(sales["City"].unique())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Final Cleaned Sales Perfomance dataset sales_clean

# COMMAND ----------

# Create a copy of the joined sales table
sales_clean = sales.copy()

# COMMAND ----------

# Check the shape of both DataFrames - sales and sales_clean
print("Original sales:", sales.shape)
print("Sales clean:", sales_clean.shape)

# COMMAND ----------



# COMMAND ----------

# Replace missing categorical values with "Unknown"
categorical_columns = [
    "PaymentMethod",
    "Status",
    "ProductName",
    "Category",
    "City",
    "CustomerSegment",
    "PaymentStatus"
]

sales_clean[categorical_columns] = (
    sales_clean[categorical_columns].fillna("Unknown")
)

# COMMAND ----------

# Check missing discounts before cleaning all numerical data
print("Missing discounts before cleaning:",
      sales_clean["Discount"].isnull().sum())


# COMMAND ----------

# Replace missing discounts with 0 - I will treat all Null discount values as not having recieved discount
sales_clean["Discount"] = sales_clean["Discount"].fillna(0)

# COMMAND ----------

# Check missing discounts after cleaning
print("Missing discounts after cleaning:",
      sales_clean["Discount"].isnull().sum())

# COMMAND ----------

# Re-calculate revenue using the cleaned discount
sales_clean["Revenue"] = (
    sales_clean["Quantity"] *
    sales_clean["UnitPrice"] *
    (1 - sales_clean["Discount"])
).round(2)

# COMMAND ----------

# Recreate Year and Month from the valid dates
sales_clean["Year"] = sales_clean["OrderDate"].dt.year
sales_clean["Month"] = sales_clean["OrderDate"].dt.month

# COMMAND ----------

# Remove records missing essential analysis fields
sales_clean = sales_clean.dropna(
    subset=[
        "OrderID",
        "Discount",
        "CustomerID",
        "ProductID",
        "OrderDate",
        "Quantity",
        "UnitPrice",
        "Revenue"
    ]
).copy()


# COMMAND ----------

# Display the final table
display(sales_clean)


# COMMAND ----------

# Verify removal of all missing values
sales_clean.isnull().sum()

# COMMAND ----------

# Re-Check total remaining missing values
print("Total nulls:", sales_clean.isnull().sum().sum())

# COMMAND ----------

# Check final rows and columns
print("Final shape:", sales_clean.shape)

## Observations:
 # Rows have decreased from 50,000 to 49655 because o the removal of all null values on the new sales_clean table 

# COMMAND ----------

# Check duplicate records
print("Duplicate rows:", sales_clean.duplicated().sum())

# COMMAND ----------

# Check duplicates before removal
print("Duplicates before:", sales_clean.duplicated().sum())

# COMMAND ----------

# Check data types
sales_clean.info()

# COMMAND ----------

display(sales_clean)

# COMMAND ----------

sales_clean.shape

# COMMAND ----------

print("Total rows:",len(sales_clean))
print("Duplicate rows:",sales_clean.duplicated().sum())
print("Missing OrderIDs:",sales_clean["OrderID"].isna().sum())

# COMMAND ----------

#Checking if City Standardization is applied
print(sales_clean["City"].unique())

## Applied

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 5: Answer the business questions

# COMMAND ----------

# MAGIC %md
# MAGIC ###### What I will do in this section:
# MAGIC - Calculate overall sales performance
# MAGIC - Look at yearly and monthly trends
# MAGIC - Product ans category performance
# MAGIC - Customer and city performance
# MAGIC - Payment performance
# MAGIC - Discount analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #######1. Overall sales performance
# MAGIC How much revenue did the shop make, from how many orders, and what is the average order value?

# COMMAND ----------

# DBTITLE 1,Calculating the overall KPIs
# Overall sales Performance - KPIs

# a. Calculating total revenue
total_revenue = sales_clean["Revenue"].sum()

# COMMAND ----------

# b. Count unique orders
total_orders = sales_clean["OrderID"].nunique()

# COMMAND ----------

# c. Calculating total quantity sold
total_quantity = sales_clean["Quantity"].sum()

# COMMAND ----------

# d. Calculating average order value
average_order_value = total_revenue/total_orders

# COMMAND ----------

# Show results of KPIs
print(f"Total Revenue: {total_revenue:,.2f}")
print(f"Total Orders: {total_orders:,.2f}")
print(f"Total Quantity Sold: {total_quantity:,.2f}")
print(f"Average Order Value: {average_order_value:,.2f}")

# COMMAND ----------

# DBTITLE 1,Create a KPI summary table
# Creating a summary table of overall sales KPIs
overall_kpis = pd.DataFrame({
    "Metric": [
        "Total Revenue",
        "Total Orders",
        "Total Quantity Sold",
        "Average Order Value"
    ],
    "Value": [
        total_revenue,
        total_orders,
        total_quantity,
        average_order_value
    ]
})

# COMMAND ----------

display(overall_kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC #######`2`. Monthly sales trend
# MAGIC  Is revenue growing, shrinking or flat month by month? Are there any seasonal peaks?

# COMMAND ----------

# DBTITLE 1,Monthly trend summary
# Grouping sales by year and month
monthly_sales = (
    sales_clean.groupby(["Year", "Month"]).agg(
        Total_Revenue=("Revenue", "sum"),
        Total_Orders=("OrderID","nunique"),
        Total_Quantity=("Quantity", "sum")
    )
).reset_index()

# Sort chronologically
monthly_sales = monthly_sales.sort_values(["Year", "Month"])

display(monthly_sales)

## Observation:
 # This table gives me three measures: 
  # Total Revenue = How much the shop makes each month
  # Total Orders = How many unique orderd are placed each month
  # Total Quantity = How many units are sold each month
# These measures will help calculate month-on-month trend over 30 months, 2024, 2025 & 6months of 2026

# COMMAND ----------

# DBTITLE 1,Calculating month-on-month revenue
# Calculate previous month's revenue
monthly_sales["Previous_Month_Revenue"] = (monthly_sales["Total_Revenue"].shift(1))

# Calculate revenue growth percentage 
monthly_sales["Revenue_Growth_%"] = ((monthly_sales["Total_Revenue"] - monthly_sales["Previous_Month_Revenue"])/ monthly_sales["Previous_Month_Revenue"])*100 

display(monthly_sales)

##Observations:
 # Revenue growth has a fluctuation of negative and positive growth
 #  There is no 0% revenue growth in the results
  # 0% would mean no change in the growth
  # positive % means revenue increased from previous month
  # negative % means revenue decreased from previous month

# COMMAND ----------

# Calculate average revenue growth
average_growth = monthly_sales["Revenue_Growth_%"].mean()

print(average_growth)

# COMMAND ----------

sales_clean = sales_clean.sort_values(["City"])

sales_clean["Revenue_Growth_%"] = (sales_clean.groupby("City")["Revenue"].pct_change()*100)

average_growth = sales_clean.groupby("City")["Revenue_Growth_%"].mean()

print(average_growth)

# COMMAND ----------

# Round Revenue_Growth percentage to two decimal places 
monthly_sales["Revenue_Growth_%"] = (monthly_sales["Revenue_Growth_%"].round(2))

display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC #######3. Revenue by Product and Category
# MAGIC - Which products and categories generate the most revenue?
# MAGIC - Which products sell the most units, and are they the same products generating the most revenue?

# COMMAND ----------

# Check column names in sales_clean
print(sales_clean.columns.tolist())

# COMMAND ----------

# Grouping sales data by product and category to identify individual product performance per category
product_sales = (sales_clean.groupby(["ProductName", "Category"], as_index=False).agg(
    Total_Revenue=("Revenue", "sum"),
    Total_Quantity=("Quantity", "sum"),
    Total_Orders=("OrderID", "nunique")
).sort_values(by="Total_Revenue",ascending=False))

display(product_sales)

## This table represents revenue by products summary, the original products table has 20 unique products


# COMMAND ----------

# Group sales data by category - Revenue, quantity, orders by category
category_sales = (sales_clean.groupby([ "Category"], as_index=False).agg(
    Total_Revenue=("Revenue", "sum"),
    Total_Quantity=("Quantity", "sum"),
    Total_Orders=("OrderID", "nunique")
).sort_values(by="Total_Revenue",ascending=False))

display(category_sales) 

## Observavtions:
 # There are 6 categories 'Electronics', 'Accessories', 'Home Office', 'Wearables', 'Gaming', and 'Stationery'
 # They are sorted by the total revenue descending 
 # Electronics has the highest revenue contribution although it has a lower quantity and order volume compared to Accessoriees 
 # Stationery is the least revenue contributor, but has a higher quantity and orders volume than Home Office, Wearables and Gaming
 # This means that quantity volume and orders increase does not mean there will be high revenue

# COMMAND ----------

# Validating total revenue
print("KPI Total Revenue:", sales_clean["Revenue"].sum())
print("Product Revenue:", product_sales["Total_Revenue"].sum())
print("Category Revenue:", category_sales["Total_Revenue"].sum())

# COMMAND ----------

# a. Quantity sold by product
 ## Highest- selling product
highest= product_sales.loc[product_sales["Total_Quantity"].idxmax()]

## Lowest-selling product
lowest= product_sales.loc[product_sales["Total_Quantity"].idxmin()]

print("Higest-selling product:", highest["ProductName"])
print("Quanity sold:", highest["Total_Quantity"])

print("Lowest-selling product:", lowest["ProductName"])
print("Quantity sold:", lowest["Total_Quantity"])

# COMMAND ----------

# b. Highest and Lowest-revenue products

## Highest-revenue product
display(product_sales.head(1)) # Headphones

## Lowest-revenue product
display(product_sales.tail(1)) # Monitor


# COMMAND ----------

# d. Comparing product quantity and revenue 
product_comparison = product_sales[["ProductName", "Total_Quantity", "Total_Revenue"]].sort_values(by="Total_Quantity", ascending=False)

display(product_comparison)

## Observations:
 # Highest quantity sold = Notebook (10,372), revenue = R 67,211.9
 # Highest revenue contributer = Headphones (R 336,078.75), quantity sold = 4842 
 # The products selling the most units are not also the ones generating the most revenue


# COMMAND ----------

## Top 5 products by revenue
top_revenue_products = product_sales.sort_values(by="Total_Revenue", ascending=False).head()

display(top_revenue_products)

# COMMAND ----------

# Calculating revenue per unit
product_sales["Revenue_Per_Unit"] = (product_sales["Total_Revenue"]/product_sales["Total_Quantity"])

display(product_sales[["ProductName", "Total_Quantity", "Total_Revenue", "Revenue_Per_Unit"]].sort_values(by="Revenue_Per_Unit", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ######## Observations:
# MAGIC - The products with the highest sales volume (quanities sold) are not necessarily the products generating the highest revenue.
# MAGIC - This indicates that sales volume alone does not explain revenue performance,
# MAGIC - Difference in revenue generated per unit may contribute to this validation
# MAGIC > - The product with the highest revenue per unit is the Tablet
# MAGIC > - Revenue per unit helps explian differences in revenue contribution across products

# COMMAND ----------

# MAGIC %md
# MAGIC #######4. Cities and Customer Segment
# MAGIC - Which cities generate the most revenue?
# MAGIC - Which custoner segments (New, Regular and VIP) are the most valuable?

# COMMAND ----------

# Verifying standardization of City
print(sales_clean["City"].unique())

# COMMAND ----------

print(sales_clean.columns.tolist())

# COMMAND ----------

# DBTITLE 1,4.1 Revenue by city
# Grouping sales by city - Calculate revenue and orders by city
city_sales = sales_clean.groupby("City").agg(
    Total_Revenue=("Revenue", "sum"),
    Total_Orders=("OrderID", "nunique"),
    Total_Quanity=("Quantity", "sum")).reset_index()

# Calculate average order value
city_sales["Average_Order_Value"] = (city_sales["Total_Revenue"]/ city_sales["Total_Orders"])

city_sales = city_sales.sort_values(by="Total_Revenue", ascending=False)

display(city_sales)

# COMMAND ----------

# DBTITLE 1,4.2 Revenue by Customer Segment
# Grouping sales by customer segment - Revenue and orders by customer segment
segment_sales = sales_clean.groupby("CustomerSegment").agg(
    Total_Revenue=("Revenue", "sum"),
    Total_Orders= ("OrderID", "nunique"),
    Total_Quantity=("Quantity", "sum"),
    Total_Customers=("CustomerID", "nunique")).reset_index()

# Calcualate average order value
segment_sales["Average_Order_Value"] = (segment_sales["Total_Revenue"]/segment_sales["Total_Orders"])



# COMMAND ----------

# Calculate revenue contribution
segment_sales["Revenue_Share_%"] = (segment_sales["Total_Revenue"]/segment_sales["Total_Revenue"].sum() * 100).round(2)

# Sort by revenue 
segment_sales = segment_sales.sort_values(by="Total_Revenue", ascending=False)

display(segment_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Observations
# MAGIC - Revenue by City
# MAGIC > - Tehran has the overall highest revenue, total orders made and quantity, although its average order value 68.83 is lower than Ashvaz 72.20 even though it takes seventh place 
# MAGIC > - Tehran's revenue of  R963,777.7 is followed by Masshad at R438,027.7. The difference is R 525,750, this is a 54.55% difference
# MAGIC > - Tehran's total revenue, orders and units sold sets it to be an outlier as all other cities are within the range of R163,124 t0 R438,027
# MAGIC - Regular customers generate the highest revenue, make most orders and buy lots of units(50,210), this also be because they are more, 5534 out of 10,000. They make up 55.68% of the revenue contribution
# MAGIC > - They are followed by New customers at 34.57% revenue contribution,and with 32,609 units. They both have a slightly equal average order value, Regular = R70.29 and New = R70.05
# MAGIC > - This means that the average revenue generated by each order between these two customer segements has only a R0.24 difference, although the total customers and revenue in each segment is different
# MAGIC - Regular customners purchase the highest total quantity. Accounting fpr 5120 units
# MAGIC - VIP customers contribute 9.69% of revenue.
# MAGIC - One unknown customer by segment has an average order value of 66.36 which is not very far from the VIP customers, although they are 968

# COMMAND ----------

# MAGIC %md
# MAGIC #######5. Order cancellations, returns and paynment failures
# MAGIC - What percentage of orders are cancelled or returned?
# MAGIC - What percebtage of payments fail?
# MAGIC - Do some payment methhods fail more than others?

# COMMAND ----------

# DBTITLE 1,5.1 Identify relevant columns
# Columns related to order status and payments
[col for col in sales_clean.columns if any(word in col.lower() for word in["status", "payment", "method", "return", "cancel"])]

# COMMAND ----------

# See unique order status
sales_clean["Status"].unique()

# COMMAND ----------

sales_clean["Status"].value_counts()

# COMMAND ----------

# DBTITLE 1,5.2Calculate percentage of cancelled and returned
# Cancelled and Returned orders percentage
cancelled_returned = sales_clean["Status"].isin(["Cancelled", "Returned"])

total_orders = len(sales_clean)
cancelled_returned_count = cancelled_returned.sum()

share = (cancelled_returned_count/total_orders)*100

print("Total orders:", total_orders)
print("Cancelled and Returned:", cancelled_returned_count)
print("Share(%):",round(share,2))

# COMMAND ----------

sales_clean[["Status", "PaymentStatus", "PaymentMethod"]].isnull().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ###