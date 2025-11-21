# Databricks notebook source
# MAGIC %md
# MAGIC ### scv_dim_prod_uom_conversion

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-dev`.demand_planning.scv_dim_prod_uom_conversion;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `pdm-pdm-gsc-bi-dev`.demand_planning.scv_dim_prod_uom_conversion (
# MAGIC   row_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   product_name STRING,
# MAGIC   uom_src STRING,
# MAGIC   uom_target STRING,
# MAGIC   conv_factor DECIMAL(15,4),
# MAGIC   note STRING,
# MAGIC   source_system STRING,
# MAGIC   date_created TIMESTAMP
# MAGIC   )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_dim_products

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-dev`.demand_planning.scv_dim_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `pdm-pdm-gsc-bi-dev`.demand_planning.scv_dim_products (
# MAGIC   row_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   productid BIGINT,
# MAGIC   productname STRING,
# MAGIC   therapeuticclass STRING,
# MAGIC   productdesc STRING,
# MAGIC   product_family STRING,
# MAGIC   evg_flag STRING,
# MAGIC   ftc_flag STRING,
# MAGIC   taf_tdf_flag STRING,
# MAGIC   npi_mature_flag STRING,
# MAGIC   source_system STRING,
# MAGIC   valid_from TIMESTAMP,
# MAGIC   valid_to TIMESTAMP,
# MAGIC   is_valid STRING,
# MAGIC   date_created TIMESTAMP,
# MAGIC   created_by STRING,
# MAGIC   lastupdatedby STRING,
# MAGIC   lastupdatedon TIMESTAMP,
# MAGIC   dp_alt_product_name STRING,
# MAGIC   patent_expiration_us STRING,
# MAGIC   patent_expiration_eu STRING,
# MAGIC   is_product_strategy STRING,
# MAGIC   demand_outlook_uom STRING,
# MAGIC   mpd_product_name_copy STRING,
# MAGIC   mpd_brand_copy STRING,
# MAGIC   mpd_therapeutic_area_copy STRING,
# MAGIC   mpd_evg_flag_copy STRING,
# MAGIC   mpd_ftc_flag_copy STRING,
# MAGIC   mpd_taf_tdf_flag_copy STRING,
# MAGIC   mpd_npi_mature_flag_copy STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_dim_country

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-dev`.demand_planning.scv_dim_country;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `pdm-pdm-gsc-bi-dev`.demand_planning.scv_dim_country (
# MAGIC   row_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   countryid BIGINT,
# MAGIC   iso_numeric_code BIGINT,
# MAGIC   iso_country_code2 STRING,
# MAGIC   iso_country_code3 STRING,
# MAGIC   iso_country_name STRING,
# MAGIC   iso_country_formal_name STRING,
# MAGIC   region_name STRING,
# MAGIC   sub_region_name STRING,
# MAGIC   gvault_country_code STRING,
# MAGIC   gvault_country_name STRING,
# MAGIC   gpid_country_name STRING,
# MAGIC   lastupdatedby STRING,
# MAGIC   lastupdatedon TIMESTAMP,
# MAGIC   source_system STRING,
# MAGIC   date_created TIMESTAMP,
# MAGIC   created_by STRING,
# MAGIC   excl_fr_regulatory_constraint STRING,
# MAGIC   ml_cluster STRING,
# MAGIC   ml_country_code_copy STRING,
# MAGIC   ml_country_copy STRING,
# MAGIC   ml_sub_region_copy STRING,
# MAGIC   ml_region_copy STRING,
# MAGIC   ml_cluster_copy STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_stage_dp_preprocess

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-dev`.demand_planning.scv_stage_dp_preprocess;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `pdm-pdm-gsc-bi-dev`.demand_planning.scv_stage_dp_preprocess (
# MAGIC   row_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   mprep_country_src STRING,
# MAGIC   mprep_product_name_src STRING,
# MAGIC   mprep_sku_src STRING,
# MAGIC   mprep_country_target STRING,
# MAGIC   mprep_product_name_target STRING,
# MAGIC   mprep_sku_target STRING,
# MAGIC   mprep_note STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_fact_master_product

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-dev`.demand_planning.scv_fact_master_product;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `pdm-pdm-gsc-bi-dev`.demand_planning.scv_fact_master_product (
# MAGIC   row_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   f_mp_id BIGINT,
# MAGIC   sku STRING,
# MAGIC   country STRING,
# MAGIC   sku_detail STRING,
# MAGIC   product_name STRING,
# MAGIC   primary_uom STRING,
# MAGIC   mto_mts_flag STRING,
# MAGIC   partner_flag STRING,
# MAGIC   tender_flag STRING,
# MAGIC   sample_flag STRING,
# MAGIC   other_flag STRING,
# MAGIC   sku_status STRING,
# MAGIC   mat_type STRING,
# MAGIC   exclude_flag STRING,
# MAGIC   note STRING,
# MAGIC   custom_field_1 STRING,
# MAGIC   custom_field_2 STRING,
# MAGIC   custom_field_3 STRING,
# MAGIC   custom_field_4 STRING,
# MAGIC   custom_field_5 STRING,
# MAGIC   custom_field_6 STRING,
# MAGIC   custom_field_7 STRING,
# MAGIC   custom_field_8 STRING,
# MAGIC   custom_field_9 STRING,
# MAGIC   custom_field_10 STRING,
# MAGIC   custom_field_11 STRING,
# MAGIC   custom_field_12 STRING,
# MAGIC   custom_field_13 STRING,
# MAGIC   custom_field_14 STRING,
# MAGIC   custom_field_15 STRING,
# MAGIC   date_created TIMESTAMP,
# MAGIC   created_by STRING,
# MAGIC   mp_part_num_country_code STRING,
# MAGIC   mp_part_num_country STRING,
# MAGIC   mp_prod_country STRING,
# MAGIC   mp_prod_country_code STRING,
# MAGIC   mp_sub_region STRING,
# MAGIC   mp_region STRING,
# MAGIC   mp_country_code STRING,
# MAGIC   mp_global_sku_segment STRING,
# MAGIC   mp_region_sku_segment STRING,
# MAGIC   mp_cluster STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_stage_future_actuals

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-dev`.demand_planning.scv_stage_future_actuals;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `pdm-pdm-gsc-bi-dev`.demand_planning.scv_stage_future_actuals (
# MAGIC   row_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   duedate STRING,
# MAGIC   partnumber STRING,
# MAGIC   country STRING,
# MAGIC   product STRING,
# MAGIC   futureactuals BIGINT,
# MAGIC   created_date TIMESTAMP,
# MAGIC   created_by STRING,
# MAGIC   modified_date TIMESTAMP,
# MAGIC   modified_by STRING,
# MAGIC   sales_uom_code STRING,
# MAGIC   standard_uom_code STRING,
# MAGIC   customer_name STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_demantra_staging_data

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-dev`.demand_planning.scv_demantra_staging_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `pdm-pdm-gsc-bi-dev`.demand_planning.scv_demantra_staging_data (
# MAGIC   row_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   sdate TIMESTAMP,
# MAGIC   level1 STRING,
# MAGIC   level2 STRING,
# MAGIC   level3 STRING,
# MAGIC   level4 STRING,
# MAGIC   level5 BIGINT,
# MAGIC   level6 STRING,
# MAGIC   level7 STRING,
# MAGIC   ebs_bh_req_qty_rd BIGINT,
# MAGIC   sdata4 BIGINT,
# MAGIC   fcst_hyp_financial BIGINT,
# MAGIC   cen_inv_adjustment BIGINT,
# MAGIC   cen_hyp_ttl_dm_fcst BIGINT,
# MAGIC   gil_total_dem_fcst BIGINT,
# MAGIC   gil_hyp_fcst_override BIGINT,
# MAGIC   gil_upside_fcst_override BIGINT,
# MAGIC   gil_grand_total_fcst BIGINT,
# MAGIC   gil_add_gps_fcst BIGINT,
# MAGIC   gil_add_gps_inv_adj BIGINT,
# MAGIC   gil_andean_fcst BIGINT,
# MAGIC   gil_andean_inv_adj BIGINT,
# MAGIC   creation_date STRING,
# MAGIC   last_update_date TIMESTAMP,
# MAGIC   batch_id BIGINT,
# MAGIC   process_flag STRING,
# MAGIC   err_message STRING,
# MAGIC   additional_info1 STRING,
# MAGIC   additional_info2 STRING,
# MAGIC   additional_info3 STRING,
# MAGIC   additional_info4 STRING,
# MAGIC   additional_info5 STRING,
# MAGIC   additional_info6 STRING,
# MAGIC   additional_info7 STRING,
# MAGIC   additional_info8 STRING,
# MAGIC   additional_info9 STRING,
# MAGIC   active STRING,
# MAGIC   published_date TIMESTAMP,
# MAGIC   etl_added_ts TIMESTAMP,
# MAGIC   load_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### scv_demantra_staging_data

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-dev`.demand_planning.scv_demantra_staging_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `pdm-pdm-gsc-bi-dev`.demand_planning.scv_demantra_staging_data (
# MAGIC   row_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   sdate TIMESTAMP,
# MAGIC   level1 STRING,
# MAGIC   level2 STRING,
# MAGIC   level3 STRING,
# MAGIC   level4 STRING,
# MAGIC   level5 DOUBLE,
# MAGIC   level6 STRING,
# MAGIC   level7 STRING,
# MAGIC   ebs_bh_req_qty_rd DOUBLE,
# MAGIC   sdata4 DOUBLE,
# MAGIC   fcst_hyp_financial DOUBLE,
# MAGIC   cen_inv_adjustment DOUBLE,
# MAGIC   cen_hyp_ttl_dm_fcst DOUBLE,
# MAGIC   gil_total_dem_fcst DOUBLE,
# MAGIC   gil_hyp_fcst_override DOUBLE,
# MAGIC   gil_upside_fcst_override DOUBLE,
# MAGIC   gil_grand_total_fcst DOUBLE,
# MAGIC   gil_add_gps_fcst DOUBLE,
# MAGIC   gil_add_gps_inv_adj DOUBLE,
# MAGIC   gil_andean_fcst DOUBLE,
# MAGIC   gil_andean_inv_adj DOUBLE,
# MAGIC   creation_date STRING,
# MAGIC   last_update_date TIMESTAMP,
# MAGIC   batch_id DOUBLE,
# MAGIC   process_flag STRING,
# MAGIC   err_message STRING,
# MAGIC   additional_info1 STRING,
# MAGIC   additional_info2 STRING,
# MAGIC   additional_info3 STRING,
# MAGIC   additional_info4 STRING,
# MAGIC   additional_info5 STRING,
# MAGIC   additional_info6 STRING,
# MAGIC   additional_info7 STRING,
# MAGIC   additional_info8 STRING,
# MAGIC   additional_info9 STRING,
# MAGIC   active STRING,
# MAGIC   published_date TIMESTAMP,
# MAGIC   etl_added_ts TIMESTAMP,
# MAGIC   load_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

