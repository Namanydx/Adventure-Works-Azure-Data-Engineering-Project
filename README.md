# Adventure-Works-Azure-Data-Engineering-Project
End-to-end Azure Data Engineering project:
This project ingests, transforms, and delivers data for Business Intelligence (BI) using Azure Data Factory, Azure Databricks, Azure Synapse Analytics, and Power BI. The data originates from the AdventureWorks dataset, retrieved directly from GitHub. The solution is organized as follows:
<img width="858" alt="Image" src="https://github.com/user-attachments/assets/58cc0a74-973d-4530-888e-aa433c44cd1c" />

# Architecture Overview
# Step 1: Azure Environment Setup ⚙️
The project begins by provisioning key Azure services:

Azure Data Factory (ADF): Facilitates data orchestration and workflow automation.

Azure Storage Account: Serves as the data lake, storing data across three layers — raw (bronze), cleaned/transformed (silver), and curated (gold).

Azure Databricks: Executes data transformation and computation tasks.

Azure Synapse Analytics: Functions as the data warehouse for Business Intelligence reporting.

All resources were configured with appropriate Identity and Access Management (IAM) roles to ensure secure and efficient integration across services.

