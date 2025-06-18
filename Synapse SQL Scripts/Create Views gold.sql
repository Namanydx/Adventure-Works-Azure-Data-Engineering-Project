--  CREATE VIEW CALENDAR --
-----------------------
CREATE VIEW gold.calendar
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://awdatalakee.blob.core.windows.net/silver/AdventureWorks_Calendar/',
        FORMAT = 'PARQUET'
    )as QUERY1

--  CREATE VIEW CUSTOMERS --
-----------------------
CREATE VIEW gold.customers
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://awdatalakee.blob.core.windows.net/silver/AdventureWorks_Customers/',
        FORMAT = 'PARQUET'
    )as QUERY1

--  CREATE VIEW PRODUCT CATEGORIES --
-----------------------
CREATE VIEW gold.procat
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://awdatalakee.blob.core.windows.net/silver/AdventureWorks_Product_Categories/',
        FORMAT = 'PARQUET'
    )as QUERY1

--  CREATE VIEW SALES 15 --
-----------------------
CREATE VIEW gold.sales
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://awdatalakee.blob.core.windows.net/silver/AdventureWorks_df_sales/',
        FORMAT = 'PARQUET'
    )as QUERY1

--  CREATE VIEW Products --
-----------------------
CREATE VIEW gold.products
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://awdatalakee.blob.core.windows.net/silver/AdventureWorks_Products/',
        FORMAT = 'PARQUET'
    )as QUERY1


--  CREATE VIEW Returns --
-----------------------
CREATE VIEW gold.returns
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://awdatalakee.blob.core.windows.net/silver/AdventureWorks_returns/',
        FORMAT = 'PARQUET'
    )as QUERY1


--  CREATE VIEW Territories --
-----------------------
CREATE VIEW gold.territories
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://awdatalakee.blob.core.windows.net/silver/AdventureWorks_Territories/',
        FORMAT = 'PARQUET'
    )as QUERY1

--  CREATE VIEW Product Subcategories --
-----------------------
CREATE VIEW gold.subcat
AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'https://awdatalakee.blob.core.windows.net/silver/Product_Subcategories/',
        FORMAT = 'PARQUET'
    )as QUERY1