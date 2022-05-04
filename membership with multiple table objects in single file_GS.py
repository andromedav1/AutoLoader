# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC This function parses membership json file into multiple objects. Currently each object is splitting out into separate columns and then pushed into a bronze format using
# MAGIC .withColumn('concatenated_cols',concat_ws('~~', *spdf_Membership_cols))
# MAGIC 
# MAGIC There are 2 displays per object just for informational purposes.
# MAGIC The first display is multiple columns (silver style) and the second is a single column (bronze style).
# MAGIC The column deltaName is added for identification. Would love to know if there is a better way of doing this.
# MAGIC 
# MAGIC A lot of hard coding in the function below to extract the information required.
# MAGIC If there is a dynamic way of doing it an example would be great.

# COMMAND ----------

# MAGIC %md
# MAGIC Commands 4, 5 and 6 are part of out standard config notebook so no changes required.

# COMMAND ----------

OPT_DS_LISTS  = ['storedProcs','tables']

OPT_TBL_COLS = ['waterMarkDateCol']
OPT_TBL_COLS_DEFAULT = ['']

OPT_TBL_LISTS = ['repeating_group', 'redactCols', 'redactVals']

# COMMAND ----------

def read_json_config_datasets(
    env: str
    , config_filepath: str
    , reqEnabled: bool=False
) -> DataFrame:
    
    assert type(env) == str, f"Expect str type for 'env', got {type(env)}"
    assert env.lower() in ['dev','prod'], f"Expect ['dev','prod'] values for 'env', got {env}"
    
    config_df = (
        spark.read
        .format('json')
        .option("multiline",True)
        .load(config_filepath) 
        .withColumn('server', when(lit(env) == 'dev', col('devServer')).otherwise(col('prodServer')))
        .withColumn('dataset', when(lit(env) == 'dev', col('dataset')).otherwise(col('prodDataset')))
        .withColumn('deltaServer', regexp_replace(col('server'), '\\\\', '.'))
        .withColumnRenamed('enabled', 'ds_enabled')
    )
            
    if reqEnabled:
        config_df = config_df.where('ds_enabled = true')
    
    return config_df

# COMMAND ----------


def read_json_config_tables(
    env: str
    , config_filepath: str
    , reqEnabled: bool=False
    , bronzeEnabled: bool=False
    , silverEnabled: bool=False
    , schedule: str=''
) -> DataFrame:
    
    
    
    config_df = read_json_config_datasets(env, config_filepath, reqEnabled)
    config_df = (
        config_df
        .select('*', explode('tables').alias('table'))     #implicit if condition using itemType column
        .select('server','deltaServer', 'dataset', 'deltaDB', 'ds_enabled','table.*')
        .withColumn('enabled', col('ds_enabled') & col('enabled'))
        .drop('ds_enabled')
        .withColumn('bronze', col('enabled') & col('bronze'))
        .withColumn('silver', col('bronze') & col('silver'))
        .withColumn('expose', col('silver') & col('expose')) 
        .withColumn('table_schema', array_union(col('table_schema'),array(lit('CORRUPTED STRING'))))
        .withColumn('schema', array_join('table_schema', ', '))
        .withColumn('deltaPartition', hash('deltaServer', 'deltaDB', 'deltaName', 'server','dataset','name'))
        .withColumn('softDeleted', lit(False))

    )
    
    for optCol, optDef in zip(OPT_TBL_COLS, OPT_TBL_COLS_DEFAULT):
        if optCol not in config_df.columns:
            config_df = config_df.withColumn(optCol, lit(optDef))  # Do this first before next line to avoid Null type issue.
            config_df = config_df.withColumn(optCol, when(col(optCol) == '', None).otherwise(col(optCol)))  
            
    for optCol in OPT_TBL_LISTS:
        if optCol in config_df.columns:
            config_df =  config_df.withColumn(optCol, when(col(optCol).isNull(), array()).otherwise(col(optCol)))
        else:
            config_df = config_df.withColumn(optCol, array())
            
    
    agg_columns = config_df.columns
    
    config_df = (
      config_df
      .select('*', explode('table_schema').alias('table_schema_field'))
      .withColumn('fields', split(col('table_schema_field'), ' ').getItem(0))
      .withColumn('field_types', split(col('table_schema_field'), ' ').getItem(1))
      .drop('table_schema_field')
      .groupBy(*agg_columns)
      .agg(collect_list('fields').alias('fields'), collect_list('field_types').alias('field_types'))
    )

    if schedule != '' and schedule is not None:
        config_df = config_df.where(f"schedule = '{schedule}'")
    
    if reqEnabled:
        config_df = config_df.where('enabled = true')
        
    if bronzeEnabled:
        config_df = config_df.where('bronze = true')
        
    if silverEnabled:
        config_df = config_df.where('silver = true')

    return config_df

# COMMAND ----------

# MAGIC %md
# MAGIC The below are the variables we use to run the notebooks. Please modify the locations to suit.

# COMMAND ----------

dbutils.fs.rm(checkpoint_path,recurse=True)
dbutils.fs.rm("/FileStore/versor/config/membership/_schema",recurse=True)
dbutils.fs.rm("/FileStore/versor/bronze",recurse=True)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType
env = 'dev'

# Please chage this to the location of the config file
config_filepath = 'dbfs:/FileStore/versor/config/membership/alms_config_mra.json'
reqEnabled = True

# Please change the below to a suitable location
inbound_source_path  = "dbfs:/FileStore/versor/streaming/membership/"
inbound_source_schema_path  = "dbfs:/FileStore/versor/config/membership/_schema"
bronze_mount_path   = "dbfs:/FileStore/versor/bronze/"
silver_mount_path   = "sdbfs:/FileStore/versor/silver/"
silver_pii = 'dbfs:/FileStore/versor/ALMS/silver/pii_lookup'
checkpoint_path = "dbfs:/FileStore/versor/config/membership/checkpoint/"



# COMMAND ----------

# MAGIC %md
# MAGIC This command is creating a dataframe based on our configs. The goal is to have the majority of the lakehouse to bronze to silver logic created through this.
# MAGIC - config_df.deltaName describes the table objects embedded in a single membership json file. (For testing this is only displaying 2 of those tables)
# MAGIC - config_df.name is the json file related to point 1

# COMMAND ----------

#dispatch config file with more metadata

# config_df = (
#         read_json_config_tables(env, config_filepath, reqEnabled=True, bronzeEnabled=True)
#         .withColumn('inbound_tablepath', concat(lit(f'{inbound_mount_path}'), col('name')))
#         .withColumn('bronze_tablepath', concat(lit(f'{bronze_mount_path}'), col('deltaDB'), lit('/'), col('deltaName')))
#         .withColumn('silver_tablepath', concat(lit(f'{silver_mount_path}'), col('deltaDB'), lit('/'), col('deltaName')))
#         .withColumn('fileType', lit ('json'))
#         .withColumn('checkpoint_path', concat(lit(f'{checkpoint_path}'), col('deltaDB'), lit('/checkpoint/'), col('deltaName'))) 
# )
# config_distinct_df  = config_df.select("name", "inbound_tablepath", "deltaDB", "fileType", array().alias("redactCols")).distinct() #table.inbound_tablepath, f'{table.deltaDB}', f'{table.fileType}', f'{table.redactCols}
# display(config_df)
# display(config_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Please change the filename variable below

# COMMAND ----------

filename = (f"{inbound_source_path}MDE_MQ_SERVICE_VEL_dev_MDE_MEMBERSHIP_ID_414d5120514d48312e50445420202020dfc0576212844026_D2022_04_1.json")
print(filename)

# COMMAND ----------

# DBTITLE 1,This function parses membership json file into multiple table objects
def parse_membership(
      bronze_df: DataFrame,
      batchId: str
):

    app_id = "Versor_app"
    MDEdata_df = bronze_df.select("serviceMDEData.*")
    MDEdata_df = MDEdata_df.withColumn("programCode", lit('VEL'))
    

    spdf_membership = MDEdata_df.select("membership.*")
    #spdf_membership.printSchema()
    spdf_mem_flatten1=spdf_membership\
                    .select("data.contact.*", "data.enrolmentDate", "data.enrolmentSource", "data.id", "data.individual.*", 
                            "data.mainTier.*", "data.membershipId", "data.meta.*", "data.status.*", "data.subtype", "included.*"
                           )

    #Membership Dataframe Silver
    spdf_Membership=spdf_mem_flatten1\
        .selectExpr("id as membership_id",\
                    "membershipid as membership_number",\
                    "subtype as type",\
                    "identity.name.romanized.title as title",\
                    "identity.name.romanized.firstName as first_name",\
                    "identity.name.romanized.lastName as last_name",\
                    "identity.name.romanized.firstName as preferred_name",\
                    "identity.birthDate as birth_date",\
                    "identity.gender as gender",\
                    "enrolmentsource as enrolment_source_id",\
                    "created.via.online.channel as enrolment_channel_id",\
                    "enrolmentDate as enrolment_date",\
                    "main as main_status_id",\
                    "main as legacy_member_status_id",\
                    "effectiveAt as effective_at",\
                    "sub.accountIdentifier.activity as activity_status_id",\
                    "sub.accountIdentifier.merge as merge_status_id",\
                    "sub.accountIdentifier.fraudSuspicion as fraud_suspicion_status_id",\
                    "sub.accountIdentifier.billing as billing_status_id",\
                    "sub.accountIdentifier.completeness.percentage as completeness_percentage",\
                    "sub.accountIdentifier.accountLoginStatus as account_login_status",\
                    "sub.characteristicIdentifier.lifeCycle.isDeceased as is_deceased",\
                    "sub.characteristicIdentifier.lifeCycle.ageGroup as age_group_id",\
                    "created.at as created_at",\
                    "created.by.system as created_by_system",\
                    "created.by.Organization as created_by_organisation",\
                    "created.by.sign as created_by_sign",\
                    "created.by.Username as created_by_user_name",\
                    "created.by.OfficeId as created_by_office_id",\
                    "created.via.online.channel as via_online_channel",\
                    "version.current as version_current"
                   )
    spdf_Membership = spdf_Membership.withColumn("deltaName", lit("membership"))
    
#   spdf_Membership = convert_to_bronze(spdf_Membership)
    spdf_Membership_cols = spdf_Membership.columns
    spdf_Membership_cols.remove("deltaName")
    spdf_Membership = spdf_Membership.withColumn('concatenated_cols',concat_ws('~~', *spdf_Membership_cols))
    spdf_Membership = spdf_Membership.drop(*spdf_Membership_cols)
    spdf_Membership = spdf_Membership.crossJoin(bronze_df)
    spdf_Membership = spdf_Membership.drop(col("value"))
    spdf_Membership = spdf_Membership.withColumnRenamed('concatenated_cols', 'value')
    
    
    #mem_bronze_table_name = spdf_Membership.first().deltaName
    #spdf_Membership.select("deltaName","value").write.mode("append").format("delta").save(path=f"dbfs:/FileStore/versor/bronze/membership/")

    #Address Dataframe
    spdf_addresses=spdf_mem_flatten1.select(spdf_mem_flatten1.id, explode_outer(spdf_mem_flatten1.addresses))
    spdf_addresses1=spdf_addresses.select(spdf_addresses.id, explode_outer(spdf_addresses.col.lines))
    spdf_MembershipContactAddress=spdf_addresses\
        .selectExpr("id as membership_id",\
                    "col.category as category",\
                    "col.lines[0] as line1",\
                    "col.lines[1] as line2",\
                    "col.lines[2] as line3",\
                    "col.countryCode as country_code",\
                    "col.cityName as city"
                   )
    spdf_MembershipContactAddress = spdf_MembershipContactAddress.withColumn("deltaName", lit("membership_address"))

    
    spdf_MembershipContactAddress_cols = spdf_MembershipContactAddress.columns
    spdf_MembershipContactAddress_cols.remove("deltaName")
    spdf_MembershipContactAddress = spdf_MembershipContactAddress.withColumn('concatenated_cols',concat_ws('~~', *spdf_MembershipContactAddress_cols))
    spdf_MembershipContactAddress = spdf_MembershipContactAddress.drop(*spdf_MembershipContactAddress_cols)
    spdf_MembershipContactAddress = spdf_MembershipContactAddress.crossJoin(bronze_df)
    spdf_MembershipContactAddress = spdf_MembershipContactAddress.drop(col("value"))
    spdf_MembershipContactAddress = spdf_MembershipContactAddress.withColumnRenamed('concatenated_cols', 'value')
    
    #display(spdf_Membership.select("deltaName","value"))
    #display(spdf_MembershipContactAddress.select("deltaName","Value"))
    #print(spdf_Membership.count())
    #print(spdf_MembershipContactAddress.count())
    #memcontadd_bronze_table_name = spdf_MembershipContactAddress.first().deltaName

    # WRITE FINAL OUTPUT IN RESPECTIVE DELTA TABLES
    spdf_Membership.select("deltaName","value").write.mode("append").format("delta").option("txnVersion",batchId).option("txnAppId",app_id).save(path=f"dbfs:/FileStore/versor/bronze/membership/")
    spdf_MembershipContactAddress.select("deltaName","value").write.mode("append").format("delta").option("txnVersion", batchId).option("txnAppId", app_id).save(path=f"dbfs:/FileStore/versor/bronze/membership_address/")

   
    #return {spdf_Membership.first().deltaName: spdf_Membership, spdf_MembershipContactAddress.first().deltaName: spdf_MembershipContactAddress, spdf_MembershipContactPhone.first().deltaName: spdf_MembershipContactPhone, spdf_MembershipContactEmail.first().deltaName: spdf_MembershipContactEmail, spdf_MembershipIndividualConsent.first().deltaName: spdf_MembershipIndividualConsent}
    #return {spdf_Membership, spdf_MembershipContactAddress}

# COMMAND ----------

#df = spark.read.format("json").load(inbound_source_path)
#print(df.count())

#dbutils.fs.ls(inbound_source_path)

# COMMAND ----------

# DBTITLE 1,Autoloader
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option("cloudFiles.schemaEvolutionMode", "rescue") \
  .option("cloudFiles.schemaLocation", inbound_source_schema_path) \
  .option("cloudFiles.inferColumnTypes", "true") \
  .option("cloudFiles.useIncrementalListing","true") \
  .load(inbound_source_path)

df.writeStream \
     .format("delta") \
     .foreachBatch(parse_membership) \
     .option("checkpointLocation",checkpoint_path) \
     .trigger(processingTime= '5 seconds') \
    .start()

# COMMAND ----------

display(spark.read.load("/FileStore/versor/bronze/membership"))


# COMMAND ----------

display(spark.read.load("/FileStore/versor/bronze/membership_address"))

# COMMAND ----------



# COMMAND ----------


