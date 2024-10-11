# DWH Project

This project is about building a Data Warehouse on a PostgreSQL Server for an E-commerce Store that also has a physical store.

## DATASETS DESCRIPTION

Our transactional dataset spans from 01/12/2022 to 09/12/2023, capturing a comprehensive overview of retail activities within the UK market. It encompasses detailed information across various dimensions:

### Online Store Dataset
1. **Product Information:**
   - **ProductID:** Unique identifier for each product sold in the online store, used to manage inventory and sales tracking.
   - **ProductName:** Name or description of the product sold online.
   - **Category:** Category or type of the product, helping classify it for easier search and analysis.

2. **Sales Information:**
   - **InvoiceID:** Unique identifier for each invoice generated from online purchases.
   - **Quantity:** The number of units of the product sold in each online transaction.
   - **EventDate:** Date and time when the online order (event) was recorded.
   - **UnitPrice:** Price of the product per unit in pound sterling.

3. **Customer Information:**
   - **CustomerID:** Unique identifier for each customer who makes an online purchase.
   - **Country:** The country where the customer resides.
   - **CustomerFirstName:** First name of the customer making the purchase.
   - **CustomerLastName:** Last name of the customer.
   - **CustomerAddress:** The physical address of the customer making the purchase.

4. **Additional Information:**
   - **PromotionID:** Unique identifier for promotions that apply to online purchases.
   - **PromoType:** Type of promotion applied.
   - **PromoStartDate:** Start date of the promotional offer.
   - **PromoEndDate:** End date of the promotional offer.
   - **Percentage:** Percentage of the discount applied during the promotion.
   - **PromoDescription:** Description of the promotional event.

### Offline Store Dataset
1. **Product Information:**
   - **ProductID:** Unique identifier for each product sold in the offline store.
   - **ProductName:** Name or description of the product sold offline.
   - **Category:** Category or type of product for easy classification.

2. **Sales Information:**
   - **InvoiceID:** Unique identifier for each invoice generated from offline store purchases.
   - **Quantity:** The number of units sold in the offline transaction.
   - **EventDate:** Date and time when the transaction occurred in the store.
   - **UnitPrice:** Price of the product per unit in pound sterling.

3. **Customer Information:**
   - **CustomerID:** Unique identifier for each customer who makes offline purchases.
   - **Country:** The country where the offline customer resides.
   - **CustomerFirstName:** First name of the customer who makes the offline purchase.
   - **CustomerLastName:** Last name of the customer.
   - **CustomerAddress:** The physical address of the offline customer.

4. **Store Information:**
   - **StoreID:** Unique identifier for the offline store where the transaction occurred.
   - **StoreAddress:** The physical address of the offline store.
   - **EmployeeID:** Unique identifier for the employee who processed the transaction.
   - **EmployeeFirstName:** First name of the employee who handled the offline purchase.
   - **EmployeeLastName:** Last name of the employee.
   - **EmployeeAddress:** Physical address of the employee.

## Below is the Chart of the Data Flow.

![Data Flow Chart](https://github.com/javohir-samandarov/DWH-Project/blob/master/Images/Data%20Flow%20Chart.PNG)

The process consists of the following layers:

1. **Data Sources:**
   The system begins with two types of source files: `online_sales.csv` and `offline_sales.csv`. These files contain data from online and offline sales transactions.

2. **Staging Layer:**
   Data from these files is batch loaded into the staging layer, represented by two foreign tables: `ext_offline_sales` and `ext_online_sales`.

3. **Source Tables:**
   The staging data is copied into the source tables `src_offline_sales` and `src_online_sales` in the Staging Area (SA) layer.

4. **Cleansing Layer:**
   After loading into source tables, the data undergoes additional manipulations in the Cleansing Layer to ensure consistency and quality before moving to the next stage.

5. **BL_3NF Entities:**
   The cleaned data is transferred to the Business Layer (BL) in 3NF (Third Normal Form) tables. Below is the schema of the BL_3NF layer:
   ![BL_3NF Schema](https://github.com/javohir-samandarov/DWH-Project/blob/master/Images/BL_3NF.png)

6. **Surrogate Key Creation:**
   During this process, surrogate keys are created to uniquely identify each record.

7. **BL_DM (Dimensional Model) Dimensions:**
   The 3NF data is further loaded into the BL_DM layer. Below is the schema of the BL_DM layer:
   ![BL_DM Schema](https://github.com/javohir-samandarov/DWH-Project/blob/master/Images/BL_DM.png)

   These dimension tables are used for analysis in the data warehouse.
