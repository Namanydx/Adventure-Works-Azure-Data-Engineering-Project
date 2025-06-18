# Adventure-Works-Azure-Data-Engineering-Project
End-to-end Azure Data Engineering project:
This project ingests, transforms, and delivers data for Business Intelligence (BI) using Azure Data Factory, Azure Databricks, Azure Synapse Analytics, and Power BI. The data originates from the AdventureWorks dataset, retrieved directly from GitHub. The solution is organized as follows:
<img width="858" alt="Image" src="https://github.com/user-attachments/assets/58cc0a74-973d-4530-888e-aa433c44cd1c" />

# Architecture Overview
# Step 1: Azure Environment Setup ⚙️
The project begins by provisioning key Azure services:

**Azure Data Factory (ADF):** Facilitates data orchestration and workflow automation.

**Azure Storage Account:** Serves as the data lake, storing data across three layers — raw (bronze), cleaned/transformed (silver), and curated (gold).

**Azure Databricks:** Executes data transformation and computation tasks.

**Azure Synapse Analytics:** Functions as the data warehouse for Business Intelligence reporting.

All resources were configured with appropriate Identity and Access Management (IAM) roles to ensure secure and efficient integration across services.
![Image](https://github.com/user-attachments/assets/b7d2dd41-812d-4e5d-b627-f123027e1f4f)

# Step 2: Building the Data Pipeline with Azure Data Factory (ADF) 🚀
Azure Data Factory acts as the core orchestrator for the end-to-end data pipeline.

**Dynamic Copy Activity:**
ADF retrieves data from GitHub via an HTTP connector and loads it into the bronze layer of the Azure Storage data lake.

**Parameterized Pipeline:**
Parameters were incorporated to make the pipeline flexible and easily adaptable to changes in the data source.

![Image](https://github.com/user-attachments/assets/99f11a0c-aa8f-4272-9a1c-9cf12a25d33d)

The raw data is now securely stored and prepared for the transformation process.

![Image](https://github.com/user-attachments/assets/4ae7c2eb-0f48-49ea-bafb-03113c124639)

# Step 3: Data Transformation with Azure Databricks 🔄
Azure Databricks was used to transform raw data from the bronze layer into a structured and analyzable format.

**Cluster Configuration:**
A Databricks cluster was provisioned to handle data processing efficiently.

**Data Lake Access:**
The cluster was integrated with Azure Storage to seamlessly access raw data stored in the data lake.

![Image](https://github.com/user-attachments/assets/e7abc0f8-bd31-48c8-9be0-3d4771f1ab12)

# Transformations Applied:

Standardized date formats to ensure consistency across records.

Removed invalid or incomplete entries to improve data quality.

Aggregated and concatenated data to enhance usability for analysis.

Stored the cleaned and structured data in the silver container using Parquet format for efficient storage and querying.
->
->

# Step 4: Data Warehousing with Azure Synapse Analytics 📊
Azure Synapse Analytics was used to organize and prepare the processed data for advanced analysis and Business Intelligence reporting.

**Integration with Silver Layer:**
Synapse was configured to access data directly from the silver container in Azure Storage.

**Serverless SQL Pools:**
Enabled querying capabilities without the need for pre-provisioned infrastructure.

**Database & Schema Design:**
Created SQL databases and schemas to logically structure the data.
Defined external tables and views to support efficient BI consumption and reporting.

![Image](https://github.com/user-attachments/assets/2e655651-fbfa-40bf-a101-2cba4a481c7c)

![Image](https://github.com/user-attachments/assets/89695ba2-cbe6-42b0-9a00-fe941c37af26)

The refined and structured data was subsequently stored in the gold container, ready for reporting and analysis.
![Image](https://github.com/user-attachments/assets/cc24bd2d-8a4a-41fa-ab86-fdd13d1e81a3)

# Step 5: Business Intelligence Integration 🕵️‍♂️
The final phase focused on visualizing the data and delivering insights through a BI tool.

**Power BI Connectivity:**
Power BI was connected to Azure Synapse Analytics for seamless data access.

**Dashboard & Report Development:**
Interactive dashboards and reports were created to provide stakeholders with clear, actionable insights.

![Image](https://github.com/user-attachments/assets/5a00bde0-3027-49b7-b560-25a669952713)









