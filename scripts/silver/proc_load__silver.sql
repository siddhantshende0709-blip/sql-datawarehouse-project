/*
=====================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=====================================================================================
Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to
populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.
=====================================================================================
Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;
=====================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME;
    BEGIN TRY
        /* ===================== CUSTOMER LOAD ===================== */
        PRINT'===================================';
        PRINT'Loading SILVER Layer';
        PRINT'===================================';

        PRINT'-----------------------------------';
        PRINT'Loading CRM Tables';
        PRINT'-----------------------------------';
        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date 
        )
        SELECT
            cst_id,
            cst_key,
            LTRIM(RTRIM(cst_firstname)) AS cst_firstname,
            LTRIM(RTRIM(cst_lastname)) AS cst_lastname,
            CASE 
                WHEN UPPER(LTRIM(RTRIM(cst_material_status))) = 'M' THEN 'Married'
                WHEN UPPER(LTRIM(RTRIM(cst_material_status))) = 'S' THEN 'Single'
                ELSE 'n/a'
            END AS cst_material_status,
            CASE 
                WHEN UPPER(LTRIM(RTRIM(cst_gndr))) = 'M' THEN 'Male'
                WHEN UPPER(LTRIM(RTRIM(cst_gndr))) = 'F' THEN 'Female'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
                ) AS Flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE t.Flag_last = 1;
        set @end_time = GETDATE();
        PRINT'Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        PRINT'---------------------------------------';
        
        /* ===================== PRODUCT LOAD ===================== */
        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
            SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost,0) AS prd_cost,
            CASE
                WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'M' THEN 'Mountain'
                WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'R' THEN 'Road'
                WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'S' THEN 'Other Sales'
                WHEN UPPER(LTRIM(RTRIM(prd_line))) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                DATEADD(DAY, -1,
                    LEAD(prd_start_dt) OVER (
                        PARTITION BY prd_key 
                        ORDER BY prd_start_dt
                    )
                )
                AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info;
        set @end_time = GETDATE();
        PRINT'Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        PRINT'---------------------------------------';
        
        /* ===================== SALES LOAD ===================== */
        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            CASE 
                WHEN sls_order_dt = 0 OR LEN(CONVERT(VARCHAR(8),sls_order_dt)) <> 8 
                THEN NULL 
                ELSE CAST(CONVERT(VARCHAR(8),sls_order_dt) AS DATE)
            END AS sls_order_dt,

            CASE 
                WHEN sls_ship_dt = 0 OR LEN(CONVERT(VARCHAR(8),sls_ship_dt)) <> 8 
                THEN NULL 
                ELSE CAST(CONVERT(VARCHAR(8),sls_ship_dt) AS DATE)
            END AS sls_ship_dt,

            CASE 
                WHEN sls_due_dt = 0 OR LEN(CONVERT(VARCHAR(8),sls_due_dt)) <> 8 
                THEN NULL 
                ELSE CAST(CONVERT(VARCHAR(8),sls_due_dt) AS DATE)
            END AS sls_due_dt,

            CASE 
                WHEN sls_sales IS NULL 
                    OR sls_sales <= 0
                    OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,

            sls_quantity,

            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END AS sls_price

        FROM bronze.crm_sales_details;
        set @end_time = GETDATE();
        PRINT'Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        PRINT'---------------------------------------';

        /* ===================== ERP CUSTOMER LOAD ===================== */
        PRINT'----------------------------------';
        PRINT'Loading ERP Tables';
        PRINT'----------------------------------';
        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
        SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END AS cid,
        CASE WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN ' Female'
            WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN ' Male'
            ELSE 'n/a'
        END AS gen
        FROM bronze.erp_cust_az12
        set @end_time = GETDATE();
        PRINT'Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        PRINT'---------------------------------------';

        /* ===================== ERP LOCATION LOAD ===================== */
        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid,cntry)
        SELECT
        REPLACE(cid,'-','') cid,
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
        FROM bronze.erp_loc_a101
        set @end_time = GETDATE();
        PRINT'Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        PRINT'---------------------------------------';

        /* ===================== ERP CATERY LOAD ===================== */
        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
        SELECT
        id,
        cat,
        subcat,
        maintenance
        FROM bronze.erp_px_cat_g1v2
        set @end_time = GETDATE();
        PRINT'========================================';
        PRINT'LOADING SILVER LAYER COMPLETED';
        PRINT'========================================';
        PRINT'Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        PRINT'========================================';
    END TRY
    BEGIN CATCH
    PRINT'===================================================='
    PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
    PRINT 'Error Message' + ERROR_MESSAGE();
    PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
    PRINT'====================================================='
    END CATCH
END
