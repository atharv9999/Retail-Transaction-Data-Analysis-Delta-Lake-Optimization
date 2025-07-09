# **🛒 Retail Transaction Data Analysis & Delta Lake Optimization**

## **📌 Project Summary**

This project showcases a robust, production-style data pipeline built on **Azure Databricks** for analyzing retail transaction data across **web**, **mobile**, and **in-store** channels. The pipeline performs **data ingestion, enrichment, analysis, Delta table optimization, and quality checks**.

It adheres to the **medallion architecture** (Bronze → Silver → Gold) and leverages **Apache Spark** with **Delta Lake** to enable scalable, reliable, and high-performance analytics.

## **🎯 Problem Statement**

A retail company needs to consolidate and analyze transaction, product, customer, and store data scattered across multiple sources and channels. The primary goals of this project are to:

* Load raw transaction data from Azure Data Lake Storage Gen2.  
* Join this data with customer, product, and location master datasets.  
* Extract meaningful business insights (e.g., country-wise revenue by channel, product trends, customer spending).  
* Store enriched data and insights efficiently using the Delta Lake format.  
* Optimize the data layout for enhanced query performance.  
* Implement robust data quality checks to ensure data reliability for decision-making.

## **🧱 Tech Stack**

| Component | Technology |
| :---- | :---- |
| Cloud Platform | Microsoft Azure |
| Compute | Azure Databricks (Single Node Cluster) |
| Storage | Azure Data Lake Storage Gen2 |
| Processing Engine | Apache Spark with PySpark |
| Format | Delta Lake |
| Language | Python |
| Orchestration (optional) | Azure Data Factory (ADF) |

## **🗂️ Dataset Overview**

The project utilizes four main datasets:

### **1\. transactions.csv**

| Column | Description |
| :---- | :---- |
| tid | Transaction ID |
| cid | Customer ID |
| pid | Product ID |
| quantity | Quantity purchased |
| amount | Total amount paid |
| tdate | Transaction date |
| channel | Transaction channel (web/mobile/store) |
| payment\_type | Payment method |
| t\_locid | Location ID where the transaction occurred |

### **2\. products.csv**

Contains product master data including pid, pname, category, brand, and base\_price.

### **3\. customers.csv**

Contains customer demographics, sign-up date, and home location (c\_locid).

### **4\. locations.csv**

Provides location metadata: locid, city, state\_region, country.

## **🛠️ Pipeline Architecture**

ADLS Gen2 (Raw CSV)  
        |  
        ▼  
Load into Spark DataFrames (Bronze Layer)  
        |  
        ▼  
Join transactions \+ customers \+ products \+ locations (Silver Layer)  
        |  
        ▼  
Cleaned \+ Enriched Data → Delta Table (Gold Layer)  
        |  
        ▼  
Extracted Business Insights \+ Optimizations

## **🔍 Key Insights Extracted**

The pipeline extracts several crucial business insights, including:

* ✅ Country-wise and channel-wise transaction count, revenue, and average order value.  
* ✅ Identification of top products by quantity sold and revenue.  
* ✅ Customer-level order analysis and total spend.  
* ✅ Revenue trends segmented by month and channel.  
* ✅ Analysis of payment type mix across different countries.

## **⚙️ Delta Lake Optimization**

Enriched data is saved as a managed Delta table:

enriched\_df.write.format("delta").mode("overwrite").saveAsTable("gold\_enriched\_transactions")

The gold\_enriched\_transactions table is optimized using ZORDER BY for improved query performance by co-locating frequently filtered columns:

OPTIMIZE gold\_enriched\_transactions  
ZORDER BY (cid, tdate, channel)

## **🧪 Data Quality Checks Performed**

Rigorous data quality checks are integrated into the pipeline, including:

* **Null value checks** on all key columns.  
* Detection of **negative quantity or amount** values.  
* Flagging of **future-dated transactions**.  
* Validation for **duplicates in transaction IDs**.  
* Verification of **logical consistency** between channel and t\_locid.

## **📊 Visualization (Optional via Databricks display())**

While optional, the project enables basic visualizations within Databricks notebooks to explore:

* Monthly revenue by channel.  
* Country-wise transaction trends.  
* Top spending customers.  
* Product category sales breakdown.

These visualizations can be further extended and integrated into tools like **Power BI** by connecting via Azure SQL Linked Service to Delta tables.

## **📁 Dat Source Structure**

📁 data/  
├── customers.csv  
├── products.csv  
├── transactions.csv  
└── locations.csv

## **✅ Final Outcomes**

This project successfully delivers:

* ⏩ Fast, optimized analytics on 10,000+ transactions leveraging Delta Lake capabilities.  
* 🔁 A reusable pipeline adaptable for processing new batches of transaction data.  
* 📈 Real, actionable business insights valuable for marketing, product, and operational teams.  
* ✅ A clear demonstration of best practices in Spark joins, partitioning, schema handling, and Delta optimization.

## **🚀 Future Improvements**

Potential enhancements for future iterations include:

* Incorporating return/refund and campaign data for deeper analytical insights.  
* Automating the pipeline via Azure Data Factory (ADF) with triggers from blob storage uploads.  
* Deploying Machine Learning models for predicting customer churn or identifying high-value segments.  
* Adding comprehensive unit tests and data validation checks using libraries like Deequ or Great Expectations.

## **Note**
As project is implemented on databricks with databricks specifi function like display(), magic %sql and charts using databricks visualizations, all this can't be exported as ipynb or python source. Here python source file is given but above given functionalities are can't be displayed.
Best option is exporting as HTML, so here HTML type file of databricks notebook is given, but as its size is 5.1MB, it can't be shown in GitHub, download it and you can see full databricks notebook content with all functionality.

## **🙋‍♂️ Author**

Atharv Kulkarni  
Intern — Data Engineering & Analytics  
📧 atharvk9999@gmail.com

## **📌 Acknowledgements**

Special thanks to the internship team and mentors for their invaluable guidance and support throughout this project.
