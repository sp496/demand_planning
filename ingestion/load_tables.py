# Databricks notebook source
import os
import json
import logging
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

 

# COMMAND ----------

# ========================================================================
# Configuration Loading
# ========================================================================

def load_config(config_path: str = "config_.json") -> dict:
    """Load and validate configuration file."""
    try:
        with open(config_path) as f:
            config = json.load(f)

        required_keys = ["raw_bucket"]
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {missing_keys}")

        logger.info("Configuration loaded successfully")
        return config

    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        raise


env = os.environ.get('DATAENV')
logger.info(f"Environment: {env}")

config = load_config()

resolved_env = "prod" if env == "prd" else env

raw_bucket = config["raw_bucket"].format(env=env)
raw_bucket_mount_point = config["raw_bucket_mount_point"].rstrip("/")
raw_data_directory = config["raw_data_directory"].rstrip("/").format(env=resolved_env)

catalog = config["catalog"].format(env=env)
schema = config["schema"]


# COMMAND ----------

# ========================================================================
# Mount S3 Buckets
# ========================================================================

def ensure_mount(mount_point: str, bucket_name: str):
    """Ensure S3 bucket is mounted."""
    if not any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
        dbutils.fs.mount(source=f"s3a://{bucket_name}", mount_point=mount_point)
        logger.info(f"Mounted {bucket_name} at {mount_point}")
    else:
        logger.info(f"Mount already exists: {mount_point}")


ensure_mount(raw_bucket_mount_point, raw_bucket)


# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_dim_prod_uom_conversion

# COMMAND ----------

# Read CSV
df = pd.read_csv(
    f"/dbfs{os.path.join(raw_bucket_mount_point, raw_data_directory, config['uom_file'])}",
    dtype=str,
    encoding='latin1'
)

# Convert pandas to spark
spark_df = spark.createDataFrame(df)

# Drop any unnamed columns
spark_df = spark_df.drop(*[col for col in spark_df.columns if col.startswith("Unnamed")])

# Clean + select + cast
spark_df = spark_df.select(
    F.trim(F.col("mu_product_name")).alias("product_name"),
    F.trim(F.col("mu_uom_target")).alias("uom_src"),
    F.trim(F.col("mu_uom_target")).alias("uom_target"),
    F.col("mu_conv_factor").cast("DECIMAL(15,4)").alias("conv_factor"),
    F.trim(F.col("mu_note")).alias("note"),
    F.lit("DEMAND PLANNING").alias("source_system"),
    F.current_timestamp().alias("date_created")
)

# Write to Delta table
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "false") \
    .saveAsTable(f"`{catalog}`.{schema}.scv_dim_prod_uom_conversion")

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_dim_products

# COMMAND ----------

# Read CSV
df = pd.read_csv(
    f"/dbfs{os.path.join(raw_bucket_mount_point, raw_data_directory, config['product_detail_file'])}",
    dtype=str,
    encoding='latin1'
)

# Convert pandas to spark
spark_df = spark.createDataFrame(df)

# Drop any unnamed columns
spark_df = spark_df.drop(*[col for col in spark_df.columns if col.startswith("Unnamed")])

# Clean + select + cast
spark_df = spark_df.select(
    F.lit(None).cast("BIGINT").alias("productid"),  # Not present in CSV
    F.trim(F.col("mpd_product_name")).alias("productname"),
    F.trim(F.col("mpd_therapeutic_area")).alias("therapeuticclass"),
    F.lit(None).alias("productdesc"),
    F.lit(None).alias("product_family"),  # No source provided
    F.trim(F.col("mpd_evg_flag")).alias("evg_flag"),
    F.trim(F.col("mpd_ftc_flag")).alias("ftc_flag"),
    F.trim(F.col("mpd_taf_tdf_flag")).alias("taf_tdf_flag"),
    F.trim(F.col("mpd_npi_mature_flag")).alias("npi_mature_flag"),
    F.lit("DEMAND PLANNING").alias("source_system"),
    F.lit(None).cast("timestamp").alias("valid_from"),
    F.lit(None).cast("timestamp").alias("valid_to"),
    F.lit(None).alias("is_valid"),
    F.current_timestamp().alias("date_created"),
    F.lit(None).alias("created_by"),
    F.lit(None).alias("lastupdatedby"),
    F.lit(None).cast("timestamp").alias("lastupdatedon"),
    F.lit(None).alias("patent_expiration_us"),
    F.lit(None).alias("patent_expiration_eu"),
    F.lit(None).alias("is_product_strategy"),
    F.lit(None).alias("demand_outlook_uom"),
    F.trim(F.col("mpd_product_name_copy")).alias("mpd_product_name_copy"),
    F.trim(F.col("mpd_brand_copy")).alias("mpd_brand_copy"),
    F.trim(F.col("mpd_therapeutic_area_copy")).alias("mpd_therapeutic_area_copy"),
    F.trim(F.col("mpd_evg_flag_copy")).alias("mpd_evg_flag_copy"),
    F.trim(F.col("mpd_ftc_flag_copy")).alias("mpd_ftc_flag_copy"),
    F.trim(F.col("mpd_taf_tdf_flag_copy")).alias("mpd_taf_tdf_flag_copy"),
    F.trim(F.col("mpd_npi_mature_flag_copy")).alias("mpd_npi_mature_flag_copy")
)

# Write to Delta table
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "false") \
    .saveAsTable(f"`{catalog}`.{schema}.scv_dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_dim_country

# COMMAND ----------

# Read CSV
df = pd.read_csv(
    f"/dbfs{os.path.join(raw_bucket_mount_point, raw_data_directory, config['location_file'])}",
    dtype=str,
    encoding='latin1'
)

# Convert pandas to spark
spark_df = spark.createDataFrame(df)

# Drop any unnamed columns
spark_df = spark_df.drop(*[col for col in spark_df.columns if col.startswith("Unnamed")])

# Clean + select + cast
spark_df = spark_df.select(
    F.lit(None).cast("BIGINT").alias("countryid"),
    F.lit(None).cast("BIGINT").alias("iso_numeric_code"),
    F.lit(None).alias("iso_country_code2"),
    F.lit(None).alias("iso_country_code3"),
    F.lit(None).alias("iso_country_name"),
    F.lit(None).alias("iso_country_formal_name"),
    F.lit(None).alias("region_name"),
    F.lit(None).alias("sub_region_name"),
    F.lit(None).alias("gvault_country_code"),
    F.lit(None).alias("gvault_country_name"),
    F.lit(None).alias("gpid_country_name"),
    F.lit(None).alias("lastupdatedby"),
    F.lit(None).cast("timestamp").alias("lastupdatedon"),
    F.lit("DEMAND PLANNING").alias("source_system"),
    F.current_timestamp().alias("date_created"),
    F.lit(None).alias("created_by"),
    F.lit(None).alias("excl_fr_regulatory_constraint"),
    F.trim(F.col("ml_cluster")).alias("ml_cluster"),
    F.trim(F.col("ml_country_code_copy")).alias("ml_country_code_copy"),
    F.trim(F.col("ml_country_copy")).alias("ml_country_copy"),
    F.trim(F.col("ml_sub_region_copy")).alias("ml_sub_region_copy"),
    F.trim(F.col("ml_region_copy")).alias("ml_region_copy"),
    F.trim(F.col("ml_cluster_copy")).alias("ml_cluster_copy")
)

# Write to Delta table
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "false") \
    .saveAsTable(f"`{catalog}`.{schema}.scv_dim_country")

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_stage_dp_preprocess

# COMMAND ----------

# Read CSV
df = pd.read_csv(
    f"/dbfs{os.path.join(raw_bucket_mount_point, raw_data_directory, config['preprocess_file'])}",
    dtype=str,
    encoding='latin1'
)

# Convert pandas to spark
spark_df = spark.createDataFrame(df)

# Drop any unnamed columns
spark_df = spark_df.drop(*[col for col in spark_df.columns if col.startswith("Unnamed")])

# Clean + select + cast
spark_df = spark_df.select(
    F.trim(F.col("mprep_country_src")).alias("mprep_country_src"),
    F.trim(F.col("mprep_product_name_src")).alias("mprep_product_name_src"),
    F.trim(F.col("mprep_sku_src")).alias("mprep_sku_src"),
    F.trim(F.col("mprep_country_target")).alias("mprep_country_target"),
    F.trim(F.col("mprep_product_name_target")).alias("mprep_product_name_target"),
    F.trim(F.col("mprep_sku_target")).alias("mprep_sku_target"),
    F.trim(F.col("mprep_note")).alias("mprep_note")
)

# Write to Delta table
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "false") \
    .saveAsTable(f"`{catalog}`.{schema}.scv_stage_dp_preprocess")

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_fact_master_product

# COMMAND ----------

# Read CSV
df = pd.read_csv(
    f"/dbfs{os.path.join(raw_bucket_mount_point, raw_data_directory, config['product_file'])}",
    dtype=str,
    encoding='latin1'
)

# Convert pandas to spark
spark_df = spark.createDataFrame(df)

# Drop any unnamed columns
spark_df = spark_df.drop(*[col for col in spark_df.columns if col.startswith("Unnamed")])

# Clean + select + cast
spark_df = spark_df.select(
    F.lit(None).cast("BIGINT").alias("f_mp_id"),
    F.trim(F.col("mp_sku")).alias("sku"),
    F.trim(F.col("mp_country")).alias("country"),
    F.trim(F.col("mp_sku_detail")).alias("sku_detail"),
    F.trim(F.col("mp_product_name")).alias("product_name"),
    F.trim(F.col("mp_primary_uom")).alias("primary_uom"),
    F.trim(F.col("mp_mto_mts_flag")).alias("mto_mts_flag"),
    F.trim(F.col("mp_partner_flag")).alias("partner_flag"),
    F.trim(F.col("mp_tender_flag")).alias("tender_flag"),
    F.trim(F.col("mp_sample_flag")).alias("sample_flag"),
    F.trim(F.col("mp_other_flag")).alias("other_flag"),
    F.trim(F.col("mp_sku_status")).alias("sku_status"),
    F.trim(F.col("mp_mat_type")).alias("mat_type"),
    F.trim(F.col("mp_exclude_flag")).alias("exclude_flag"),
    F.trim(F.col("mp_note")).alias("note"),
    F.lit(None).alias("custom_field_1"),
    F.lit(None).alias("custom_field_2"),
    F.lit(None).alias("custom_field_3"),
    F.lit(None).alias("custom_field_4"),
    F.lit(None).alias("custom_field_5"),
    F.lit(None).alias("custom_field_6"),
    F.lit(None).alias("custom_field_7"),
    F.lit(None).alias("custom_field_8"),
    F.lit(None).alias("custom_field_9"),
    F.lit(None).alias("custom_field_10"),
    F.lit(None).alias("custom_field_11"),
    F.lit(None).alias("custom_field_12"),
    F.lit(None).alias("custom_field_13"),
    F.lit(None).alias("custom_field_14"),
    F.lit(None).alias("custom_field_15"),
    F.current_timestamp().alias("date_created"),
    F.lit(None).alias("created_by"),
    F.trim(F.col("mp_part_num_country_code")).alias("mp_part_num_country_code"),
    F.trim(F.col("mp_part_num_country")).alias("mp_part_num_country"),
    F.trim(F.col("mp_prod_country")).alias("mp_prod_country"),
    F.trim(F.col("mp_prod_country_code")).alias("mp_prod_country_code"),
    F.trim(F.col("mp_sub_region")).alias("mp_sub_region"),
    F.trim(F.col("mp_region")).alias("mp_region"),
    F.trim(F.col("mp_country_code")).alias("mp_country_code"),
    F.trim(F.col("mp_global_sku_segment")).alias("mp_global_sku_segment"),
    F.trim(F.col("mp_region_sku_segment")).alias("mp_region_sku_segment"),
    F.trim(F.col("mp_cluster")).alias("mp_cluster")
)

# Write to Delta table
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "false") \
    .saveAsTable(f"`{catalog}`.{schema}.scv_fact_master_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_stage_future_actuals

# COMMAND ----------

# Read CSV
df = pd.read_csv(
    f"/dbfs{os.path.join(raw_bucket_mount_point, raw_data_directory, config['future_orders_file'])}",
    dtype=str,
    encoding='latin1'
)

# Convert pandas to spark
spark_df = spark.createDataFrame(df)

# Drop any unnamed columns
spark_df = spark_df.drop(*[col for col in spark_df.columns if col.startswith("Unnamed")])

# Clean + select + cast
spark_df = spark_df.select(
    F.trim(F.col("Line Schedule Ship Date")).alias("duedate"),
    F.trim(F.col("Product Number")).alias("partnumber"),
    F.trim(F.col("Ship To Location Country")).alias("country"),
    F.trim(F.col("Product Name")).alias("product"),
    F.col("Ordered Quantity (Sales UoM)").cast("BIGINT").alias("futureactuals"),
    F.lit(None).cast("timestamp").alias("created_date"),
    F.lit(None).alias("created_by"),
    F.lit(None).cast("timestamp").alias("modified_date"),
    F.lit(None).alias("modified_by"),
    F.trim(F.col("Sales UOM Code")).alias("sales_uom_code"),
    F.trim(F.col("Standard UOM Code")).alias("standard_uom_code"),
    F.trim(F.col("Customer Name")).alias("customer_name")
)

# Write to Delta table
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "false") \
    .saveAsTable(f"`{catalog}`.{schema}.scv_stage_future_actuals")

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_demantra_staging_data

# COMMAND ----------

# Read CSV
df = pd.read_csv(
    f"/dbfs{os.path.join(raw_bucket_mount_point, raw_data_directory, config['forecast_data_file'])}",
    dtype=str,
    encoding='latin1'
)

# Convert pandas to spark
spark_df = spark.createDataFrame(df)

# Drop any unnamed columns
spark_df = spark_df.drop(*[col for col in spark_df.columns if col.startswith("Unnamed")])

# Clean + select + cast
spark_df = spark_df.select(
    F.col("SDATE").cast("TIMESTAMP").alias("sdate"),
    F.trim(F.col("LEVEL1")).alias("level1"),
    F.trim(F.col("LEVEL2")).alias("level2"),
    F.trim(F.col("LEVEL3")).alias("level3"),
    F.trim(F.col("LEVEL4")).alias("level4"),
    F.col("LEVEL5").cast("DOUBLE").alias("level5"),
    F.trim(F.col("LEVEL6")).alias("level6"),
    F.trim(F.col("LEVEL7")).alias("level7"),
    F.col("EBS_BH_REQ_QTY_RD").cast("DOUBLE").alias("ebs_bh_req_qty_rd"),
    F.col("SDATA4").cast("DOUBLE").alias("sdata4"),
    F.col("FCST_HYP_FINANCIAL").cast("DOUBLE").alias("fcst_hyp_financial"),
    F.col("CEN_INV_ADJUSTMENT").cast("DOUBLE").alias("cen_inv_adjustment"),
    F.col("CEN_HYP_TTL_DM_FCST").cast("DOUBLE").alias("cen_hyp_ttl_dm_fcst"),
    F.col("GIL_TOTAL_DEM_FCST").cast("DOUBLE").alias("gil_total_dem_fcst"),
    F.col("GIL_HYP_FCST_OVERRIDE").cast("DOUBLE").alias("gil_hyp_fcst_override"),
    F.col("GIL_UPSIDE_FCST_OVERRIDE").cast("DOUBLE").alias("gil_upside_fcst_override"),
    F.col("GIL_GRAND_TOTAL_FCST").cast("DOUBLE").alias("gil_grand_total_fcst"),
    F.col("GIL_ADD_GPS_FCST").cast("DOUBLE").alias("gil_add_gps_fcst"),
    F.col("GIL_ADD_GPS_INV_ADJ").cast("DOUBLE").alias("gil_add_gps_inv_adj"),
    F.col("GIL_ANDEAN_FCST").cast("DOUBLE").alias("gil_andean_fcst"),
    F.col("GIL_ANDEAN_INV_ADJ").cast("DOUBLE").alias("gil_andean_inv_adj"),
    F.trim(F.col("CREATION_DATE")).alias("creation_date"),
    F.col("LAST_UPDATE_DATE").cast("TIMESTAMP").alias("last_update_date"),
    F.col("BATCH_ID").cast("DOUBLE").alias("batch_id"),
    F.trim(F.col("PROCESS_FLAG")).alias("process_flag"),
    F.trim(F.col("ERR_MESSAGE")).alias("err_message"),
    F.trim(F.col("ADDITIONAL_INFO1")).alias("additional_info1"),
    F.trim(F.col("ADDITIONAL_INFO2")).alias("additional_info2"),
    F.trim(F.col("ADDITIONAL_INFO3")).alias("additional_info3"),
    F.trim(F.col("ADDITIONAL_INFO4")).alias("additional_info4"),
    F.trim(F.col("ADDITIONAL_INFO5")).alias("additional_info5"),
    F.trim(F.col("ADDITIONAL_INFO6")).alias("additional_info6"),
    F.trim(F.col("ADDITIONAL_INFO7")).alias("additional_info7"),
    F.trim(F.col("ADDITIONAL_INFO8")).alias("additional_info8"),
    F.trim(F.col("ADDITIONAL_INFO9")).alias("additional_info9"),
    F.trim(F.col("ACTIVE")).alias("active"),
    F.col("PUBLISHED_DATE").cast("TIMESTAMP").alias("published_date"),
    F.col("ETL_ADDED_TS").cast("TIMESTAMP").alias("etl_added_ts"),
    F.col("LOAD_DATE").cast("TIMESTAMP").alias("load_date")
)

# Write to Delta table
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "false") \
    .saveAsTable(f"`{catalog}`.{schema}.scv_demantra_staging_data")

# COMMAND ----------