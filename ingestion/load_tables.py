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

# # Write to Delta table
# spark_df.write.format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "false") \
#     .saveAsTable(f"`{catalog}`.{schema}.scv_dim_prod_uom_conversion")

spark_df.display()