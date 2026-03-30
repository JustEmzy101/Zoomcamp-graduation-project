import sys
import logging
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession,DataFrame, functions as F
from pyspark.sql.functions import lit,col,when
import pyspark.sql.utils
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, TimestampType, BooleanType
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def Reading_BQ_if_exists(spark: SparkSession ,project_id: str , dataset_id: str ,table_id:str) -> DataFrame:
    """
    This function returns the DataFrame if the table exists in BigQuery and a DataFrame with None if it doesn't eixsts
    """
    table_path = f"{project_id}:{dataset_id}.{table_id}"
    try:
        df = spark.read.format("bigquery").option("table", table_path).option("filter", "1 = 0").load()
        logging.info(f"Table {table_id} exists in BigQuery. Reading the schema...")
    except Py4JJavaError as e:
    # Check if the error is because the table doesn't exist
        if "Not found: Table" in str(e):
            logging.warning(f"Table {table_id} does not exist in BigQuery, Proceeding as new table...")
            df = None # create an empty DataFrame here
        else:
            logging.error(f"Error reading table {table_id} from BigQuery: {e}")
            raise e  # If it's a different error (like permissions), raise it!
    return df
############## Schema Comparsion function ################
def compare_schemas(df1,df2):
    """
    This function takes 2 DataFrames and compares their schemas and their type and returns 
    a descrepncies list which collects the different names/types or even an update of the schema
    whether in name or type
    """
    schema1 = df1.schema
    schema2 = df2.schema
    fields1 = {item.name: item.dataType for item in schema1}
    fields2 = {item.name: item.dataType for item in schema2}
    keys = set(fields1.keys()).union(set(fields2.keys()))
    print('keys',keys)
    logger.info(f"columns:",keys)
    discrepancies = []
    for key in keys:
        if key not in fields1:
            discrepancies.append(f"Column {key} is missing in first schema")
        if key not in fields2:
            discrepancies.append(f"Column {key} is missing in second schema")
        if key in fields1 and key in fields2 and fields1[key] != fields2[key]:
            discrepancies.append(f"Column {key} has different types: {fields1[key]} vs {fields2[key]}")
    
    return discrepancies
###############################################################
######### Schema Merging Function ##################
# Here if we solely walks on schema it will succeed but if the same column name exists for the data merge it can fail
def harmonize_schema(incoming_df, production_df):
    """
    This function takes 2 DataFrames checks the schemas of two DataFrames and adds missing columns with null values to the one with fewer columns with the same data
    type in order not to entirely break BI tools BUT BUT BUT adding 0 for an aggregated column e.g: AVG will swing these BI tools
    PS : it does break if the production column don't exist in the new data
    """
    incoming_schema = incoming_df.schema
    production_schema = production_df.schema
    for col in incoming_df.columns:
        if col not in production_df.columns:
            production_df = production_df.withColumn(col, lit(None).cast(incoming_schema[col].dataType))

    for col in production_df.columns:
        if col not in incoming_df.columns:
            incoming_df = incoming_df.withColumn(col, lit(None).cast(production_schema[col].dataType))
            logger.warning(f"Schema Breaking Issue!! Column {col} don't exist in incoming data")
            # sys.exit(1) Exit the program with a non-zero exit code use it if you want to break the pipeline 
    aligned_incoming_df = incoming_df.select(*production_df.columns)
    return aligned_incoming_df #, production_df
    
#####################################################
########## Data "OUTER JOIN" Merge #######################
# this works especially for DataFrames that has the same schema & names and that will now be merged
# here in that example created_at is duplicated in both tables but they mean different things for each table
# so, we can rice the column name for that column before joins so they are joined correctly 
def merge(df_main, df_upd, list_keys): 
    """
    This function takes 2 DataFrames and the join key(s) to join 2 dataframes that has don't have the same column and an UPSERT manner
    and returns the updated dataframe with the merge
    """
    from pyspark.sql.functions import col,when
  
    df_main_cols = df_main.columns
    df_upd_cols = df_upd.columns
    #Adds the suffix "_tmp" to df_upd columns to make unique to enable join logic to work
    add_suffix = [df_upd[f'{col_name}'].alias(col_name + '_tmp') for col_name in df_upd.columns]
    df_upd = df_upd.select(*add_suffix)

    join_condition = [col(f"{k}") == col(f"{k+ '_tmp'}") for k in list_keys]
    #Doing a full outer join
    df_join = df_main.join(df_upd, join_condition, "fullouter")
    #Using the for loop to scroll through the columns
    for col in df_main_cols:
        #Implementing the logic to update the rows
        df_join = df_join.withColumn(col, when((df_main[col] != df_upd[col + '_tmp']) | (df_main[col].isNull()), df_upd[col + '_tmp']).otherwise(df_main[col]))
        
    #Selecting only the columns of df_main (or all columns that do not have the suffix "_tmp")
    df_final = df_join.select(*df_main_cols)
    #Returns the dataframe updated with the merge
    return df_final
    ##################################################################
    ##################################################################
    ############ FIXING COLLIDING NAMES WITH DIFFERENT MEANING #######
def resolve_collisions(df_main, df_upd, join_keys, main_prefix="prod_", upd_prefix="incoming_"):
    """
    Dynamically detects and prefixes overlapping columns between two dataframes,
    excluding the keys used for joining and a prefix to be later differentiate the columns
    """
    
    # 1. Find overlapping columns
    main_cols = set(df_main.columns)
    upd_cols = set(df_upd.columns)
    common_cols = main_cols.intersection(upd_cols)
    
    # 2. Exclude the join keys from being renamed
    cols_to_rename = common_cols - set(join_keys)
    
    # If there are no collisions, just return the original dataframes
    if not cols_to_rename:
        return df_main, df_upd

    print(f"Detected collisions to rename: {cols_to_rename}")

    # 3. Build the select expressions for the Main DF (Users)
    main_exprs = [
        F.col(c).alias(f"{main_prefix}{c}") if c in cols_to_rename else F.col(c)
        for c in df_main.columns
    ]
    df_main_clean = df_main.select(*main_exprs)

    # 4. Build the select expressions for the Update DF (Bookings)
    upd_exprs = [
        F.col(c).alias(f"{upd_prefix}{c}") if c in cols_to_rename else F.col(c)
        for c in df_upd.columns
    ]
    df_upd_clean = df_upd.select(*upd_exprs)

    return df_main_clean, df_upd_clean
##########################################################
##########################################################

def read_parquet(spark: SparkSession, gcs_path: str):
    """
    A Function to connect the GCS bucket and read the parquet files
    """
    logger.info(f"Reading parquet from {gcs_path}")
    df = spark.read.parquet(gcs_path)
    logger.info(f"Schema: {df.schema}")
    logger.info(f"Row count: {df.count()}")
    return df


def apply_transformations(df):
    """
    Here we can pack our major transformations like filling nulls, adding new metadata columns, etc
    so it can be usuable template for future transformations
    """
    logger.info("Applying transformations")

    df = (
        df
        .withColumn("ingested_at",   F.current_timestamp())
    )

    logger.info(f"Row count after transforms: {df.count()}")
    return df


def write_to_bq(df, gcp_project: str, bq_dataset: str, bq_table: str, gcs_temp_bucket: str):
    """
    A function to write the dataframe to BigQuery directly 
    """
    destination = f"{gcp_project}.{bq_dataset}.{bq_table}"
    logger.info(f"Writing to BigQuery: {destination}")

    (
        df.write
        .format("bigquery")
        .option("table",                destination)
        .option("temporaryGcsBucket",   gcs_temp_bucket)   # BQ connector stages data here first
        .option("writeMethod",          "indirect")         # streams via GCS → BQ
        .mode("overwrite")                                     
        .save()
    )
    logger.info("Write to BigQuery complete")

########################################################################
########################################################################
########################################################################


def main():

    gcp_project     = sys.argv[1]
    gcs_input_path  = sys.argv[2]   # e.g. gs://my-bucket/data/input/
    bq_dataset      = sys.argv[3]
    bq_table        = sys.argv[4]
    gcs_temp_bucket = sys.argv[5]
    gcs_quarantine_path = sys.argv[6]   # This the quarantine bucket for failed writes to BQ or MAJOR issue

    spark = SparkSession.builder \
        .appName("gcs-to-bq")\
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")\
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/etc/gcp/key.json")\
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
        .config("parentProject", gcp_project)\
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")\
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")\
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "true")\
        .getOrCreate()

    try:
        incoming_df = read_parquet(spark, gcs_input_path)
        logging.info(f"Read new incoming data successfully")
        logging.info(f"Schema: {incoming_df.schema}")
        logging.info(f"Reading Existing Production Data...")
        production_df = Reading_BQ_if_exists(spark,gcp_project,bq_dataset,bq_table)
        logging.info(f"Schema: {incoming_df.schema}")
        incoming_df_schema = incoming_df.schema
        production_df_schema = production_df.schema
        discrepancies = compare_schemas(incoming_df, production_df)
        logger.info(f"Differences:",discrepancies)
        if production_df == None or discrepancies != True :
            # Normal Transformation happens here
            df_transformed = apply_transformations(incoming_df)
            write_to_bq(df_transformed, gcp_project, bq_dataset, bq_table, gcs_temp_bucket)

       
        if discrepancies:
            logger.info(f"Discrepencies found: {discrepancies}")
            logger.info("Harmonizing schema...")
            harmonized_incoming_df = harmonize_schema(incoming_df, production_df)    
            df_transformed = apply_transformations(harmonized_incoming_df)
        else:
            pass


        
            
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


