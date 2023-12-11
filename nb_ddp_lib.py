# Databricks notebook source
# MAGIC %md
# MAGIC ## Function library Notebook
# MAGIC
# MAGIC Implement functions in other notebooks
# MAGIC This Notebook can be used to implement functions that can be used by other notebooks. 
# MAGIC If you want to make the functions avaiable in other notebooks you need to run this notebook first. 
# MAGIC You can do this by using the %run magic command in the notebook were you want to use the functions.
# MAGIC
# MAGIC If you run the following command in your notebook the nb_ddp_lib notebook will be executed 
# MAGIC and the functions are avaibale in the current spark session. 
# MAGIC Run the command before execute one of the following defined functions in the notebook where you want to use the functions.
# MAGIC
# MAGIC `%run nb_ddp_lib`
# MAGIC  
# MAGIC #### Contribue changes or new functions
# MAGIC If you want to change a function in this notebook and make it avaible if you execute the nb_ddp_lib from another notebook, 
# MAGIC you need to publish your changes to the synapse workspace first. Otherwise your changed are not implemented.
# MAGIC
# MAGIC You can develope new functions and test your function in this notebook, but you need to take care that there are no function executions
# MAGIC when you publish you notebook to the synapse workspace. Otherwise this functios will be executed too if you use the run magic command from another notebook.
# MAGIC
# MAGIC If done with the testing of your function you can comment the test out so that they are not executed if you run this notebook form another notebook.
# MAGIC
# MAGIC  # !! This is only a temporary solution until we have developed a python package that we can install into the spark pool !!
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import from_json, col, lit, to_timestamp, collect_list, struct, max
import pyspark.sql.functions as F 
from delta.tables import *
from datetime import datetime, timedelta
import pandas as pd
from builtins import max
import delta
import json
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException


# COMMAND ----------

logger = sc._jvm.org.apache.log4j.LogManager.getLogger("DDP - Library")

# COMMAND ----------

## Define global helper functions

# COMMAND ----------

def check_list_difference(df1_cols, df2_cols):
    s = set(df1_cols)
    differnce = [x for x in df2_cols if x not in s]
    return differnce


# COMMAND ----------

def get_storage_name():
    '''
    Get the ressource name from the synapse linked service connection

    linked_service_name (string) -> Synapse linked service name
    '''

    properties = dbutils.credentials.getPropertiesAll("ls_keyvault")

    # extract the keyvault name from the endpoint
    ls_keyvault_name = json.loads(properties)["Endpoint"].split("//")[-1][:-1]

    # get the storage account name from the key vault
    storage_name = dbutils.credentials.getSecret(ls_keyvault_name, "synapse-storage-name", "ls_keyvault")


    return storage_name

# COMMAND ----------

def ddp_lib_create_rawtable_url(container_name, storage_account, blob_path, load_type):
    return "abfss://{}@{}.dfs.core.windows.net/{}/{}".format(container_name, storage_account, blob_path, load_type)


# COMMAND ----------

def ddp_lib_create_deltatable_url(container_name, storage_account, blob_path, schema_version, load_type):
    if load_type == None:
        url = "abfss://{}@{}.dfs.core.windows.net/{}/{}".format(container_name, storage_account, blob_path, schema_version)
    else : url = "abfss://{}@{}.dfs.core.windows.net/{}/{}/{}".format(container_name, storage_account, blob_path, schema_version, load_type)
    return url

# COMMAND ----------

def ddp_lib_schema_validation(df1, df2):

    """ Check for schema cahnges

    df1 -> DeltaTable
    df2 -> extract


    """

    # get columns for dataTable and extract
    df1_cols = df1.columns
    df2_cols = df2.columns

    # get datatypes for dataTable and extract
    df1_dtypes = df1.dtypes
    df2_dtypes = df2.dtypes
    
    # all columns that are not int the DeltaTable
    col_diff_df1 = check_list_difference(df1_cols, df2_cols)

    # all columns that are not in the extract
    col_diff_df2 = check_list_difference(df2_cols, df1_cols)

    col_diff = set(col_diff_df1 + col_diff_df2)
    # dtype_diff = check_list_difference(df1_dtypes, df2_dtypes)

    if col_diff_df1==[]:
        valid = True
    else:
        valid = False

    return valid, col_diff

# COMMAND ----------

def ddp_lib_path_exists(url):
    try:
        path = dbutils.fs.ls(url)
        path_exists = True
    except: 
        path_exists = False
    return path_exists

# COMMAND ----------

def ddp_lib_path_exists(url):
    try:
        path = dbutils.fs.ls(url)
        path_exists = True
    except: 
        path_exists = False
    return path_exists

# COMMAND ----------


def ddp_lib_create_merge_dict(df_delta_table,column_difference):
    columns= df_delta_table.columns
    Updatedict = {}

    for col in columns:
        if col in column_difference:
            Updatedict["old."+col] = "null"
        else:
            Updatedict["old."+col] = "new."+col
    for col in column_difference:
        if col not in columns:
            Updatedict["new."+col] = "old."+col
    return Updatedict

def ddp_lib_merge_delta(original_delta_path, new_df, primary_keys,merge_dict):
    """
    Description: 
    Merges the new Data into the existing Data by primary Keys. If the primary key already exists, the data is updated, if the keys do not exists, the data is inserted.
    This method is called in the write-function.
    parameters
    original_delta_path (string) --> path where delat table of source in structured zone
    new_df(data frame) --> new data frame which shall be merged
    primary_keys (string) --> primary keys of source table, is needed for join condition in merge
    """
    # load the existing data table
    orDT = delta.DeltaTable.forPath(spark, original_delta_path)
    # create join clause for merge statement by primary keys
    v_join_condition = create_join_condition(primary_keys)

    # merge the deltaTable with the new DataFrame
    orDT.alias("old")\
        .merge(new_df.alias("new"), v_join_condition)\
        .whenMatchedUpdate(set = merge_dict)\
        .whenNotMatchedInsert(values = merge_dict)\
        .execute()
    orDT.toDF().show()
    return orDT

def ddp_lib_get_files_in_time_period(path, start_datetime, end_datetime):
    """
    Given a root folder in a data lake and a time period, finds all files under that folder which were created in that time period.
    
    More specifically, we assume that there is a hierarchical folder structure under the root folder which looks like
    <root folder>/yyyy/MM/dd/HH/mm/<files>.  This function then navigates down the folder structure based on the time period passed
    in, and retrieves all files which lie in folders between the two given datetimes (inclusive of the start, exclusive of the end).
    Notes / Assumptions:
        1) All five levels of folder must be present (even if only one file is present per day)
        2) The files must be live directly in the the "mm" folder; any files in a sub-folder below that will not be returned
        3) The folders can be named 1, 2, 3, etc. instead of 01, 01, 03 if desired
        4) The start and end times are treated as having a resolution up to 1 minute.  Any "seconds" or smaller part is ignored.

    Parameters:
      path:           The path to do the search (fully qualified, optionally ending with a slash: abfs://etc/etc/)
      start_datetime: The start date and time of the search (inclusive)
      end_datetime:   The end date and time of the search (exclusive)
    Returns:          A list of files which are in folders within the timespan, correctly ordered by date and time
    
    Remarks:
        Why make the start date inclusive and the end date exclusive?  Partly because experience has shown it's more natural.
        For example, to find one day's files you can search from 15th June 00:00 to 16th June 00:00 rather than have to take off
        a minute (or should that be a second?  Or a millisecond?) from the end time.  Furthermore, if we made the end date be
        inclusive instead, people might naively expect a search from 15th June to 15th June to return all files on the day,
        whereas in fact no files would be returned at all except perhaps for any which were generated exactly at midnight)
        
        On the other hand, a consequence of seconds (and below) being ignored, is that searching from 15th June 00:00:00 to
        16 June 00:00:59 will NOT find any files on 16 June 00:00.  This is a minor inconvenience, which is outweighed by the
        advantages of having the end date be exclusive.  (And arguably make sense -- if the minute hasn't finsished yet, it
        stands to reason that not all files might have been put there yet.)
    """

    file_paths = []
    if end_datetime <= start_datetime:
        raise ValueError("start_datetime must be before end_datetime")

    # Ensure the path ends with a slash
    path = path.rstrip('/') + "/"

    # The following block of code is somewhat repetitive.  It would be possible to write a shorter
    # recursive version, but the code would almost certainly be less clear
    # because of the need to extract the appropriate parts from the start and end datetimes depending
    # on the recursion level.  So we take a more manual approach.

    # Search down the appropriate folder(s) for the necessary year(s)
    year_folders = ddp_lib_get_folders_between_ints(path, start_datetime.year, end_datetime.year)
    for year_folder in year_folders:
        # Get the month folders.  This is complicated by the fact that if the time period selected
        # spans one or more year boundaries, we need to search from the start date to December in
        # the start year, from January to December in the middle years, and from January to the end
        # date in the final year.  (Of course, we're unlikely to ever be given such a large date
        # range, but exactly the same logic is used for timespans crossing multiple months, days, and
        # hours.) 
        is_first_year = int(year_folder) == start_datetime.year
        is_final_year = int(year_folder) == end_datetime.year
        start_month = start_datetime.month if is_first_year else 1
        end_month = end_datetime.month if is_final_year else 12
        year_path = path + year_folder + "/"
        month_folders = ddp_lib_get_folders_between_ints(year_path, start_month, end_month)
        for month_folder in month_folders:
            # Now get the day folders.  We use the same logic as for months
            is_first_month = is_first_year and int(month_folder) == start_datetime.month
            is_final_month = is_final_year and int(month_folder) == end_datetime.month
            start_day = start_datetime.day if is_first_month else 1
            end_day = end_datetime.day if is_final_month else 31          # 31 is fine, even in months with fewer days
            month_path = year_path + month_folder + "/"
            day_folders = ddp_lib_get_folders_between_ints(month_path, start_day, end_day)
            for day_folder in day_folders:
                # The hours folders
                is_first_day = is_first_month and int(day_folder) == start_datetime.day
                is_final_day = is_final_month and int(day_folder) == end_datetime.day
                start_hour = start_datetime.hour if is_first_day else 0
                end_hour = end_datetime.hour if is_final_day else 24      # Again, 23 should be fine but we err on the side of caution
                day_path = month_path + day_folder + "/"
                hour_folders = ddp_lib_get_folders_between_ints(day_path, start_hour, end_hour)
                for hour_folder in hour_folders:
                    # And the minutes folders.
                    # There is one small difference here.  Because we treat the end time as *exclusive*, we take one minute away from the
                    # value of end_minute for the final hour.  This might make it -1, in which case no folders will be found (which is correct)
                    is_first_hour = is_first_day and int(hour_folder) == start_datetime.hour
                    is_final_hour = is_final_day and int(hour_folder) == end_datetime.hour
                    start_minute = start_datetime.minute if is_first_hour else 0
                    end_minute = end_datetime.minute - 1 if is_final_hour else 60
                    hour_path = day_path + hour_folder + "/"
                    minute_folders = ddp_lib_get_folders_between_ints(hour_path, start_minute, end_minute)
                    for minute_folder in minute_folders:
                        # Finally, add all files (not folders) in each "minute" folder
                        minute_path = hour_path + minute_folder + "/"
                        folder_contents = dbutils.fs.ls(minute_path)
                        for folder_item in folder_contents:
                            if (folder_item.isDir): continue
                            file_paths.append(minute_path + folder_item.name)
    return file_paths

def ddp_lib_get_folders_between_ints(path, start, end):
    """
    Finds all sub-folders of the given folder whose name is an integer between start and end (inclusive)
    Parameters:
      path  - The path to do the search (fully qualified, and ending with a slash: abfs://etc/etc/)
      start - The start integer
      end   - The end integer (may be less than start, in which case the empty list will be returned)
    Returns - A list of those child folder names which can be parsed as integers, in numeric order

    Remarks: We don't just return the integers, because then the caller doesn't know whether a folder is called e.g., 01 or 1
    """
    folder_list = []
    sub_folders = dbutils.fs.ls(path)
    for sub_folder in sub_folders:
        if not(sub_folder.isDir): continue                       # We're only interested in folders...
        try:
            sub_folder_int = int(sub_folder.name)                # ...whose name is an integer
        except ValueError:
            continue
        if (sub_folder_int >= start and sub_folder_int <= end):  # ...and in the correct range
            folder_list.append(sub_folder.name)
        
    return sorted(folder_list, key = lambda y: int(y))           # Sort them numerically (so, e.g., "1" goes between "0" and "2", not between "9" and "10")

def ddp_lib_get_last_extraction_date(table_name, container_name, storage_account, schema_version):

    ''' 
    Get the latest load date for a table from the audit_log deltatable.
    This functions is used in the delta load logic

    path (string) -> path to select the table in the audit_logs.
    storage_container -> Name of the storage container
    storage_account -> Name of the storage account
    '''
    # create audit_log url
    table_url = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{table_name}/{schema_version}"
    path_exist = delta.DeltaTable.isDeltaTable(spark, table_url)
    # try to load the audit_log table. if there is no audit log table the schema version should be v_001
    if path_exist:
        # load delta table
        deltaTable = DeltaTable.forPath(spark, table_url)
        # convert delta table to dataframe
        df_table = deltaTable.toDF()
        
        # get the latest date if df_audit path i not empty
        df_table = df_table.withColumn("max_timestamp", to_timestamp(df_table.tec_extraction_date_ts_utc,'yyyy-MM-dd HH:mm:ss'))
        v_last_load = df_table.select(F.max('max_timestamp')).collect()[0][0]
    
    else:
        print("DeltaTable dont exist")
        # 
        v_last_load = None
    return v_last_load

def ddp_lib_schema_merge(df_new_schema, container_name, storage_account, blob_path, schema_version, load_type, primary_keys):
    """
    Description
    When a new schema shall be updated to the old one then this function genereates a new version number and a new folder and writes the ne deltatable and merges the old and the new 
    data in this delta table:   The delta table is then located in the new <verson_number>-folder

    df_new_schema (data frame) --> data frame which shall be merged into existing data table
    container_name (string) --> container name 
    storage_account (string) --> storage account
    blob_path (string) --> folder of source table
    schema_version (string) --> last schema_version
    load_type (string) --> full, partial, delta which comes from config file
    primary_keys (string) --> primary keys from delta table 
    """
    # create path to the deltatable with the outdated schema
    original_delta_path = ddp_lib_create_deltatable_url(container_name, storage_account, blob_path, schema_version, None)

    # load existing deltatable
    deltaTable = DeltaTable.forPath(spark, original_delta_path)

    # convert delta table to dataframe
    deltatable_df = deltaTable.toDF()

    # create a new schema version
    schema_number = int(schema_version.split("_")[-1])
    new_schema_version = "v_{:03d}".format(schema_number + 1)

    # create path to the new deltatable merged schema
    new_deltatable_url = ddp_lib_create_deltatable_url(container_name, storage_account, blob_path, new_schema_version, None)

    # Enable automatic schema evolution
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    # load the old deltatable into a dataframe (full history, from old schema)
    oldDT = delta.DeltaTable.forPath(spark, original_delta_path)
    df_old_schema = oldDT.toDF()

    # save the new schema to the adls loaction (new schema version in path)
    df_old_schema.write.format('delta').mode("overwrite").save(new_deltatable_url)
    
    # load the deltatable with the new schema (until now only with one delta, old is missing yet)
    newDT = delta.DeltaTable.forPath(spark, new_deltatable_url)

    # create the join condition with the primary keys (e.g. old.column = new.column)
    v_join_condition = create_join_condition(primary_keys)

    #merge the new schema history to the new deltatable to update all new entries
    newDT.alias("old")\
        .merge(df_new_schema.alias("new"), v_join_condition)\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
        # .option("mergeSchema", "true")\
    # Disable automatic schema evolution
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")

    return new_schema_version


# ## Define helper functions for generic write

# In[202]:


import delta

def create_join_condition(primary_keys):

    """
    Convert primary keys from config file to desired format.
    The primary key format is needed in the merge operation to
    match the old and new primary keys.

    primary_keys (string) -> primary key in format "key1, key2"

    outout (string) -> 

    """

    keys = primary_keys.split(",")

    join_condition = ""
    for i in keys:
        if len(join_condition) > 0:
            join_condition += " AND "
        join_condition += "old.{}<=>new.{}".format(i,i)
    return join_condition



def directoryExists(path_to_check):
    '''
    Check whether a directory on ADLS exists or not.
    Returns a boolean value.
    '''
    v_exists = False 
    try:
        dbutils.fs.ls(path_to_check)
        v_exists = True
    except:
        v_exists = False

    return v_exists



def extract_maxfolder(folders):
    '''
    Checks in a folder structure <year>/<month>/<day> for the maximum foldername.
    This funciton does not check recursively, so it is just called for one of the layers.
    So in data engineering speach, return the folder of the latest date.
    Returns: The folder name of the maximum date in the given structure
    '''
    folder_list = []
    for folder in folders:
        folder_list.append(folder.name) 

    return max(folder_list)


def ddp_lib_get_max_date(base_url):
    '''
    Get the maximun date from storage paths in a folder structure of in a folder structure <year>/<month>/<day>.
    Returns:
     - max_year
     - max_month
     - max_day
    '''
    
    # get latest year
    years_folder = dbutils.fs.ls(base_url)
    max_year = extract_maxfolder(years_folder)

    # get latest month
    month_folder = "{}/{}".format(base_url, max_year)
    month_folder = dbutils.fs.ls(month_folder)
    max_month = extract_maxfolder(month_folder)

    # get latest day
    day_folder = "{}/{}/{}".format(base_url, max_year, max_month) 
    day_folder = dbutils.fs.ls(day_folder)
    max_day = extract_maxfolder(day_folder)

    # get hour
    hour_folder = "{}/{}/{}/{}".format(base_url, max_year, max_month, max_day) 
    hour_folder = dbutils.fs.ls(hour_folder)
    max_hour = extract_maxfolder(hour_folder)
    # get minute
    minute_folder = "{}/{}/{}/{}/{}".format(base_url, max_year, max_month, max_day, max_hour) 
    minute_folder = dbutils.fs.ls(minute_folder)
    max_minute = extract_maxfolder(minute_folder)

    max_date_path = "{}/{}/{}/{}/{}".format(base_url, max_year, max_month, max_day, max_hour, max_minute)

    return max_year, max_month, max_day, max_hour, max_minute



# In[203]:


def delete_rows_auditlog( path, load_date, audit_container_name,audit_storage_account,audit_log_path):
   """
   Description: Deletes a row in the audit_log table in the audit log transient or structured which is controlled by the input parameters.
   We can use it for testing purposes f.e. if we create a test table during development and can delete the entries for it.
   input parameters
      path (string) -> the path which belongs to column path in the audit_log table
      load_date (datetime) -> belongs to load_date column in the audit_log table, load_date and path are unique and identifies which row you want to delete in audit log 
      audit_container_name (string) -> specifies which audit log table should be deleted: ddp-structured or ddp-transient 
      audit_storage_account (string) -> storage account where audit_log table is located
      audit_log_path (string) --> folder name of the audit-log table in container, is always called audit_log
   """
   
   audit_log_url = "abfss://{}@{}.dfs.core.windows.net/{}".format(audit_container_name, audit_storage_account, audit_log_path)
   deltaTable = DeltaTable.forPath(spark, audit_log_url)
   
   # load delta table
   deltaTable = DeltaTable.forPath(spark, audit_log_url)
   # convert delta table to dataframe
   df_audit_log = deltaTable.toDF()
   v_row_empty = deltaTable.toDF().filter((col("path") == path) and  (col("load_date")== load_date)).rdd.isEmpty()
   print(v_row_empty)
   if v_row_empty == False:
     deltaTable.delete((col("path") == "{}".format(path)) and (col("load_date")=="{}".format(load_date)))
     print("Delete row in audit_logs for {} and {} successful ".format(path, load_date))
   elif v_row_empty == True:
      print("data do not exist in audit log table") 


# In[204]:


def delete_table(container_name,storage_account,blob_path):
    """
    Description: Deletes a whole table in storage account which is controlled by the input parameters.
    We can use it for testing purposes f.e. if we create a test table during development and can delete the table.
    input parameters
    audit_container_name (string) -> specifies in which container the table shall be deleted.  
    audit_storage_account (string) -> storage account where the table is located
    blob_path (string) --> folder name of the table in container
    """
    full_url_to_delete = "abfss://{}@{}.dfs.core.windows.net/{}".format(container_name, storage_account, blob_path)
    #dbutils.fs.ls(test_url)

    dbutils.fs.rm(full_url_to_delete, True) # Set the last parameter as True to remove all files and directories recursively
    print(test_url + " deleted")


# ## Structured NB Functions

# <h4>Load</h4>

# In[205]:


def ddp_lib_read_schema(schema_url):
    '''
    Read a json file from the datalake and convert it to the pyspark
    schema format.

    All schemas are saved in the datalake at config/master/schemas

    parameters:

    path (string)       -> abfss path to the folder in the data lake where the schema are located (abfss://config@zc01xyumasynstc5fanp00.dfs.core.windows.net/schemas/)

    '''

    # check if schema exists
    try:
        schema_source = schema_url
        print(schema_source)
        rdd = spark.sparkContext.wholeTextFiles(schema_source)
        json_schema = json.loads(rdd.collect()[0][1])
        schema = StructType.fromJson(json_schema)
        
        logger.info(f"Found existing schema definitions at:\n {schema_url}")

    except Exception:
        logger.warn("No schema definition found. Fallback to schema auto detection")
        schema = None

    return schema


# In[206]:


def ddp_lib_load(url, file_type, schema=None, use_csv_headers=True, use_json_multiline=False, xlsx_options=[("header","true")]):
    """ Generic function to load different data types.

    Args:
        url (str): Url/Path to data location 
        file_type (str): File type of the data that should be loaded (delta, avro, xml, parquet, csv, json)
        schema (Schema, optional): Spark schema from json config. Can be loaded with ddp_lib_read_schema(). Defaults to None.
        use_csv_headers (bool, optional): Value to set .option("header",True) when loading csv files. Defaults to True.
        use_json_multiline (bool, optional): Value to set .option("multiline",True) when loading json files. Defaults to True.
        xlsx_options: a list of key-value pairs for loading xlsx dtype. Each key should be an acceptable/valid option name of spark.read function and values should be also valid.
                    for learn more about available options see this link:https://github.com/crealytics/spark-excel#create-a-dataframe-from-an-excel-file
                
                *** As "header" option for loading excel dtype using spark.read method is a required option, it is already passed as default key-value pair in xlsx_options parameter.
    Returns:
        DataFrame: Dataframe which is loaded from the given path by the given file_type
    """
    # file types that do not need special processing
    basic_file_types = ["xml", "parquet", "json", "csv", "xlsx"]
    try:
        logger.info(f"Data will be loaded with file_type: {file_type}")
        if file_type in basic_file_types:
            if schema:
                logger.info(f"Data is loaded with Schema")
                # load table with predefined schema
                if (file_type == "csv"):
                    # use headers as column names
                    logger.info(f"CSV load uses headers: {use_csv_headers}")
                    if (use_csv_headers):
                        df = spark.read.format(file_type).option("header", True).schema(schema).load(url)
                    else:
                        # do not use headers as column names
                        df = spark.read.format(file_type).schema(schema).load(url)
                    return df
                
                elif (file_type == "json" and use_json_multiline):
                    logger.info(f"Json load uses multiline: {use_json_multiline}")
                    # read multiline json file
                    df = spark.read.format(file_type).option("multiline", True).schema(schema).load(url)
                    return df
                
                elif (file_type == "parquet"):
                    logger.info(f"Parquet load")
                    df = spark.read.format(file_type).load(url)
                    return df

                elif (file_type == "xlsx"):
                    logger.info(f"XLSX load uses these options: {xlsx_options}")
                    options_dict = dict(xlsx_options)
                    df = spark.read.format("excel").options(**options_dict).schema(schema).load(url)
                    return df

                else:    
                    df = spark.read.format(file_type).schema(schema).load(url)
                    return df
            else:
                logger.info(f"Data is loaded without Schema")
                # load table with autodetected schema
                if (file_type == "csv"):
                    logger.info(f"CSV load uses headers: {use_csv_headers}")
                    if (use_csv_headers):
                        # use headers as column names
                        df = spark.read.format(file_type).option("header", True).load(url)
                    else:
                        # do not use headers as column names
                        df = spark.read.format(file_type).load(url)
                    return df
                
                elif (file_type == "json" and use_json_multiline):
                    logger.info(f"Json load uses multiline: {use_json_multiline}")
                    # read multiline json file
                    df = spark.read.format(file_type).option("multiline", True).load(url)
                    return df
                
                elif (file_type == "parquet"):
                    logger.info(f"Parquet load")
                    df = spark.read.format(file_type).load(url)
                    return df
                
                elif (file_type == "xlsx"):
                    logger.info(f"XLSX load uses these options: {xlsx_options}")
                    options_dict = dict(xlsx_options)
                    df = spark.read.format("excel").options(**options_dict).load(url)
                    return df

                else:
                    df = spark.read.format(file_type).load(url)
                    return df

        elif file_type == "avro":
            # read the avro file from the adls
            df_avro_raw = spark.read.format("avro").option("inferSchema", "true").load(url)

            # convert body column back into json-string
            df_body = df_avro_raw.select(df_avro_raw.Body.cast("string"))

            # auto infer schema
            json_schema_auto = spark.read.json(df_body.rdd.map(lambda row: row.Body)).schema
            json_df = df_body.withColumn("Body", F.from_json(F.col("Body"), json_schema_auto))

            # extract all columns of the json into the target DF.
            df = json_df.select(F.col("Body.*"))
            return df
        elif file_type == "delta":
            # read delta table
            df = spark.read.format("delta").load(url)
            return df
        else:
            logger.info(f"file_type {file_type} is not supported wih this method")
    except Exception as e:
        logger.error(f"Failed to load data with file_type {file_type} under path {url}")
        raise e


# <h4>Write</h4>

# In[207]:


def ddp_lib_write(
    df,
    write_mode,
    storage_account,
    container_name,
    blob_path,
    load_type,
    audit_container_name,
    audit_storage_account,
    audit_log_path,
    load_date,
    primary_keys=None):
    """
    Write data as delta table with different write modes.

    df (dataframe) ->
    write_mode (string) ->
    storage_account (string) ->
    container_name (string) ->
    blob_path (string) -> relative path inside a container. It does not contain the partitioning (year, month, day)

    """
    
    table_name = blob_path
    
    # get the latest schema version
    schema_version = ddp_lib_get_auditlog_last_schema_version(table_name=table_name, audit_log_path=audit_log_path, audit_container_name=audit_container_name, audit_storage_account=audit_storage_account)

    # define the url to the deltatable
    table_url = ddp_lib_create_deltatable_url(container_name, storage_account, blob_path, schema_version, None)
    
    

    if write_mode == "overwrite":
        
        # try to load into the existing schema version folder
        try:
            df.write.format('delta').mode(write_mode).save(table_url)
            print(table_url + " correct")
            logger.info("Output file has been written to: " + table_url)
            # log the table write
            ddp_lib_audit_logging(table_url,table_name, write_mode,load_date, schema_version, audit_log_path, audit_container_name, audit_storage_account, load_type)

        # when the schema has changed the AnalysisException error will be thrown.
        # Then the new schema version schould be loaded into a new schema version folder
        except AnalysisException:

            # count up schema version
            # v_001 -> ["v", "001"]
            new_schema_version = schema_version.split("_")[-1]
            new_schema_version = int(new_schema_version) + 1
            new_schema_version = "v_{:03d}".format(new_schema_version)
            
            # create alert that new schema version has been created
            logger.warn("Schema version updated")

            # table url for new schema version
            table_url = ddp_lib_create_deltatable_url(container_name, storage_account, blob_path, new_schema_version, None)
                        
            # write table in new schema folder
            df.write.format('delta').mode(write_mode).save(table_url)
            
            # log the table write
            ddp_lib_audit_logging(table_url,table_name, write_mode,load_date, new_schema_version, audit_log_path, audit_container_name, audit_storage_account, load_type)

    elif write_mode == "append":

        if primary_keys==None:
            raise ValueError("DDP: For merge operation the parameter 'primary_keys' must be defined")
            
        # compare schemas from existing deltatable and new dataframe
        DT = DeltaTable.forPath(spark, table_url)
        validation,col_diff = ddp_lib_schema_validation(DT.toDF(), df)
        merge_dict = ddp_lib_create_merge_dict(DT.toDF(), col_diff)

        if validation == True:
            logger.info("Start Data Append")
            df.write.format('delta').mode(write_mode).save(table_url)
            logger.info("Output file has been written to: " + table_url)
            # log the table write
            ddp_lib_audit_logging(table_url,table_name, write_mode,load_date, schema_version, audit_log_path, audit_container_name, audit_storage_account, load_type)
        else:
            # create a new schema version and run schema merge function
            logger.warn("Schema confict detected...")
            logger.warn("Move DeltaTable to new schema version")
            new_schema_version = ddp_lib_schema_merge(df, container_name, storage_account, blob_path, schema_version, load_type, primary_keys)
            logger.info("Schema conflict resolved. Output file has been written to: " + table_url)
            
            # log the table write
            ddp_lib_audit_logging(table_url,table_name, write_mode, load_date, new_schema_version, audit_log_path, audit_container_name, audit_storage_account, load_type)
            
    elif write_mode == "merge":

        if primary_keys==None:
            raise ValueError("DDP: For merge operation the parameter 'primary_keys' must be defined")
        
        # compare schemas from existing deltatable and new dataframe
        DT = DeltaTable.forPath(spark, table_url)
        validation,col_diff = ddp_lib_schema_validation(DT.toDF(), df)
        merge_dict = ddp_lib_create_merge_dict(DT.toDF(), col_diff)

        if validation == True:
            print('validation true')
            logger.info("Start Data Merge")
            result = ddp_lib_merge_delta(
                original_delta_path = table_url,
                new_df = df,
                primary_keys= primary_keys,
                merge_dict=merge_dict)
            # log the table write
            ddp_lib_audit_logging(table_url,table_name, write_mode, load_date, schema_version, audit_log_path, audit_container_name, audit_storage_account, load_type)
            logger.info("Data Merge Finished. Output file has been written to: " + table_url)
            
        elif validation == False:
            print('validation false')
            # create a new schema version and run schema merge function
            logger.warn("Schema confict detected...")
            logger.warn("Move DeltaTable to new schema version")
            new_schema_version = ddp_lib_schema_merge(df, container_name, storage_account, blob_path, schema_version, load_type, primary_keys)
            logger.info("Schema conflict resolved. Output file has been written to: " + table_url)
            # log the table write
            ddp_lib_audit_logging(table_url,table_name, write_mode, load_date, new_schema_version, audit_log_path, audit_container_name, audit_storage_account, load_type)
    


# ## Data Quality Checks

# In[208]:


def ddp_lib_rollback(target_table,output_container_name,storage_account,schema_version,write_mode,load_type,audit_blob_path="audit_log"):

    '''
    In the event that data within a table becomes corrupted/deleted this fuction will rollback to a previous version of the table, restoring the data

    target_table (string) -> name of table to rollback
    output_container_name (string) -> name of output container
    storage_account (string) -> storage account
    path (string) -> path to target table
    schema_version (string) -> schema version
    write_mode (string) -> write mode used to save the table
    load_type (string) -> load type used
    audit_blob_path (string) -> path to audit log
    '''

    url = "abfss://{}@{}.dfs.core.windows.net/{}".format(output_container_name, storage_account, target_table)

    logger.info(f"Using URL: {url}")

    current_version, rollback_version = ddp_lib_fetch_version_history(url=url)

    if(rollback_version is None):
        logger.Warn("No previous versions found, is first load?")
        return

    logger.info(f"Rolling back to version {rollback_version} from current version {current_version}")

    try:
        old_version = spark.read.format("delta").option("versionAsOf", rollback_version).load(url)
    except Exception:
        logger.error(f"Failed to roll back to version: {rollback_version}")
        return

    old_version.write.format('delta').mode("overwrite").save(url)

    latest_version, rollback_version = ddp_lib_fetch_version_history(url=url)

    logger.info(f"Current version is {latest_version}")
    logger.info("Writing to audit table...")

    current_ts = datetime.now()
    current_tf_utc = datetime.utcfromtimestamp(float(current_ts.strftime('%s')))
    ddp_lib_audit_logging(url,target_table,write_mode, current_tf_utc, schema_version, audit_blob_path, output_container_name, storage_account, load_type,latest_version,rollback_version)

#TODO: add check for length of historic, in the case where a new table is added could throw error
def ddp_lib_fetch_version_history(table_name="",output_container_name="",storage_account="",url=None):
    if(url is None):
        url = "abfss://{}@{}.dfs.core.windows.net/{}".format(output_container_name, storage_account, table_name)
    hist = DeltaTable.forPath(spark, url).history()
    historic = hist.collect()
    current_version = historic[0].version
    previous_version = historic[1].version if len(historic) > 1 else -1
    logger.info(f"current_version: {current_version}, previous_version: {previous_version}")
    return current_version,previous_version

def ddp_lib_data_quality_checks(source_container_name, output_container_name,storage_account,source_tables,target_table,schema_version,write_mode="overwrite",audit_blob_path="audit_log"):
    logger.info("Beginning data quality checks")

    latest_audit_rows = ddp_lib_fetch_latest_from_audit_log(source_container_name,storage_account,"audit_log",list(source_tables))
    
    logger.info("Successfully fetched latest audit rows")

    source_load_types = list(source_tables.values())
    load_type_flag = False

    if(latest_audit_rows != [None] and len(latest_audit_rows) > 0):
        last_load_types = list(map(lambda x: x.load_type,latest_audit_rows))
        for i in range(len(last_load_types)):
            logger.info(f"target load type: {last_load_types[i]}, source load type: {source_load_types[i]}")
            if(last_load_types[i] == source_load_types[i]):              
                load_type_flag = True
    else:
        return -1

    logger.info(f"load_type_flag = {load_type_flag}")

    if(not load_type_flag): 
        return 0

    df = "abfss://{}@{}.dfs.core.windows.net/{}".format(output_container_name, storage_account, target_table)
    logger.info(f"Loading target dataframe from: {df}")
    table = spark.read.format("delta").load(df)

    if(table.rdd.isEmpty()):
        logger.warn("Data in not good state, rolling back to previous version")
        ddp_lib_rollback(
            target_table,
            output_container_name,
            storage_account,
            schema_version,
            write_mode,
            source_load_types[0],
            audit_blob_path)
        return 1
    else:
        return 0


# ## Auditing Functions

# In[209]:


def ddp_lib_audit_logging(blob_path,table_name, write_mode, load_date, schema_version, audit_log_path, audit_container_name, audit_storage_account, load_type, latest_version=None, base_version=None):

    ''' 
    Log the table write

    blob_path (string) -> blob storage path to the written table
    write_mode (string) -> write mode used to save the table
    audit_container_name (string) -> storage container name to use save the audit logs
    audit_storage_account -> storage account name to use save the audit logs
    latest_version (optional) (int) -> this is the latest version of the table, if left empty this will be fetched and incremented from the last occurence in the audit log against the specificed table
    base_version (optional) (int) -> this is the version the table is based on, if left empty this will be fetched from the last occurence in the audit log against the specificed table
    '''
    spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

    try:
        user_name = dbutils.env.getUserName()
    except:
        user_name = "?"

    if(latest_version is None or base_version is None):
        latest_version,base_version = ddp_lib_fetch_version_history(url=blob_path)

    print(f"Latest version is: {latest_version}")

    # intialise data of lists.
    data = {'table_name':[table_name],
            'path':[blob_path],
            'load_date':[load_date],  ###### TODO: Change load_date for testing case for full/ delta load  #########
            'write_mode':[write_mode],
            'load_type' :[load_type],
            'schema_version':[schema_version],
            'user':[user_name],
            'latest_version':[latest_version],
            'base_version':[base_version]}

    print(data)

    # Create DataFrame
    log_pandas_df = pd.DataFrame(data)

    # Convert Pandas dataframe to spark DataFrame
    log_df = spark.createDataFrame(log_pandas_df)
    
    audit_log_url = "abfss://{}@{}.dfs.core.windows.net/{}".format(audit_container_name, audit_storage_account, audit_log_path)
    
    print(audit_log_url)
    #log_df.write.format('delta').mode('append').option('path', audit_log_url).save(audit_log_url)
    log_df.write.format('delta').mode('append').save(audit_log_url)

    # TODO: ADD data info to logs
    logger.info("Logged data to audit_logs ")

def ddp_lib_get_auditlog_last_loaddate(table_name, audit_log_path, audit_container_name, audit_storage_account):

    ''' 
    Get the latest load date for a table from the audit_log deltatable.
    This functions is used in the delta load logic

    path (string) -> path to select the table in the audit_logs.
    storage_container -> Name of the storage container
    storage_account -> Name of the storage account
    '''
    # create audit_log url
    audit_log_url = "abfss://{}@{}.dfs.core.windows.net/{}".format(audit_container_name, audit_storage_account, audit_log_path)
    
    # try to load the audit_log table. if there is no audit log table the schema version should be v_001
    try:
        # load delta table
        deltaTable = DeltaTable.forPath(spark, audit_log_url)
        # convert delta table to dataframe
        df_audit_log = deltaTable.toDF()

        # select all rows witch equals p_path
        df_audit_log = df_audit_log[df_audit_log.table_name == table_name]  

        # check if df_audit_log is empty
        if df_audit_log.rdd.isEmpty():
            v_last_load = None
        else:
            # get the latest date if df_audit path i not empty
            v_last_load = df_audit_log.select("load_date").rdd.max()[0]
    except:
        schema_version ="v_001"
    return v_last_load

def ddp_lib_get_auditlog_last_schema_version(table_name, audit_log_path, audit_container_name, audit_storage_account):

    ''' 
    Get the latest schema version for a table from the audit_log deltatable.
    This functions is used in the delta load logic

    path (string) -> path to select the table in the audit_logs.
    storage_container -> Name of the storage container
    storage_account -> Name of the storage account
    '''
    # create audit_log url
    audit_log_url = "abfss://{}@{}.dfs.core.windows.net/{}".format(audit_container_name, audit_storage_account, audit_log_path)
    try:
        # load delta table
        deltaTable = DeltaTable.forPath(spark, audit_log_url)
        # convert delta table to dataframe
        df_audit_log = deltaTable.toDF()

        # select all rows witch equals p_path
        df_audit_log = df_audit_log[df_audit_log.table_name == table_name]

        # check if df_audit_log is empty
        if df_audit_log.rdd.isEmpty():
            schema_version = "v_001"
        else:
            # get the latest date if df_audit path i not empty
            v_last_load = df_audit_log.select("load_date").rdd.max()[0]
            schema_version = df_audit_log.filter(df_audit_log.load_date == v_last_load).select("schema_version").first()[0]
    except:
        schema_version = "v_001"

    return schema_version


# add fetch latest load type in this
def ddp_lib_fetch_latest_from_audit_log(container_name,storage_account,audit_log_path, table_names):
    '''
    This will fetch a list of the latest audit rows for the corresponding to the table names listed in table_names

    container_name (string) -> audit log container name
    storage_account (string) -> storage account
    audit_log_path (string) -> path to audit log
    table_names (list(string)) -> a list of table names to fetch latest audit rows
    '''
    audit_log_url = "abfss://{}@{}.dfs.core.windows.net/{}".format(container_name, storage_account, audit_log_path)

    logger.info(f"Loading in audit log from {audit_log_url}")

    df = spark.read.format("delta").load(audit_log_url)
    df = df.sort(F.col("load_date").desc())

    logger.info("Audit log loaded")
        
    return list(map(lambda table_name: df.filter(F.col('table_name') == table_name).first(), table_names))

def ddp_lib_export_error_logging(blob_path,source_table_name,export_type,pipeline_run_id,timestamp,row_id,payload,error_msg,error_log_path, error_container_name, error_storage_account):
    
    '''
    Log the rows which failed to export

    blob_path (string) -> blob storage path to the written table
    source_table_name (string) -> name of source table
    pipeline_run_id (string) -> pipeline_run_id in which the failure occurred
    timestamp (datetime) -> time at which the error occurred
    payload (string) -> json payload which was going to be sent
    error_msg (string) -> error message
    '''

    try:
        user_name = dbutils.env.getUserName()
    except:
        user_name = "?"

    # intialise data of lists.
    data = {'table_name':[source_table_name],
            'path':[blob_path],
            'pipeline_run_id':[pipeline_run_id],
            'timestamp':[timestamp],
            'row_id':[row_id],
            'payload': [str(payload)],
            'error_msg':[str(error_msg)],
            'export_type':[export_type],
            'user':[user_name]}
    
    # Create DataFrame
    log_pandas_df = pd.DataFrame(data)

    # Convert Pandas dataframe to spark DataFrame
    try:
        log_df = spark.createDataFrame(log_pandas_df)
        error_log_url = "abfss://{}@{}.dfs.core.windows.net/{}".format(error_container_name, error_storage_account, error_log_path)
        log_df.write.format('delta').mode('append').save(error_log_url)
        logger.info("Logged errors to error_logs ")
    except Exception as e:
        logger.error(f"Failed to create data frame for: {data}")
        raise e
        
def ddp_lib_export_audit_logging(blob_path,table_name, export_date,successful_transfers,failed_transfers,pipeline_run_id, audit_log_path, audit_container_name, audit_storage_account, export_type,ddp_latest_date):

    ''' 
    Log the table export

    blob_path (string) -> blob storage path to the written table
    write_mode (string) -> write mode used to save the table
    audit_container_name (string) -> storage container name to use save the audit logs
    audit_storage_account -> storage account name to use save the audit logs
    '''
    
    try:
        user_name = dbutils.env.getUserName()
    except:
        user_name = "?"

    # intialise data of lists.
    data = {'table_name':table_name,
            'path':blob_path,
            'export_date':export_date,
            'export_type' :export_type, # delta/full
            'successful_transfers':successful_transfers,
            'failed_transfers':failed_transfers,
            'pipeline_run_id':pipeline_run_id,
            'ddp_latest_date':ddp_latest_date,
            'user':user_name}
    
    try:
        log_df = spark.createDataFrame([Row(**data)])

        audit_log_url = "abfss://{}@{}.dfs.core.windows.net/{}".format(audit_container_name, audit_storage_account, audit_log_path)
        log_df.write.format('delta').mode('append').save(audit_log_url)
        
        logger.info("Logged data to audit_logs ")
    except Exception as e:
        logger.error(f"Failed to create and write dataframe: {data}")
        raise e


# In[210]:


def ddp_lib_deltatable_repartition(path, num_files):
    """
    Repartion a table to a spcific number of files.
    The deltatable will be loaded from path and save again with overwrite mode
    Data will not be changed.

    path (string) -> path to the table that should be repartitioned
    num_files (int) -> number of files after the repartition
    """

    spark.read \
        .format("delta") \
        .load(path) \
        .repartition(num_files) \
        .write \
        .format("delta") \
        .option("dataChange", "false") \
        .mode("overwrite") \
        .save(path)


# In[211]:


def ddp_lib_deltatable_optimization(path):
    """
    Optimize the layout of data in storage.
    The speed of read queries from the deltatable will be improved by coalescing small files into larger ones
    Data will not be changed.

    path (string) -> path to the table that should be optimized
    """
    try:
        # run optimization
        spark.sql(f"OPTIMIZE '{path}'")
        # get the latest operation
        delta_table = delta.DeltaTable.forPath(spark, path).history(1)
        operation = delta_table.select("operation").collect()[0][0]

        if(operation == "OPTIMIZE"):
            # get operation metrics of the latest operation
            operation_metrics = delta_table.select("operationMetrics").collect()[0][0]
            num_files_removed = operation_metrics['numRemovedFiles']
            num_bytes_removed = operation_metrics['numRemovedBytes']
            num_files_added = operation_metrics['numAddedFiles']
            num_bytes_added = operation_metrics['numAddedBytes']

            logger.info(f"Successfully optimized the data under path {path} with following metrics:\n \
            numRemovedFiles: {num_files_removed} \n \
            numRemovedBytes: {num_bytes_removed} \n \
            numAddedFiles: {num_files_added} \n \
            numAddedBytes: {num_bytes_added}")
        
        else:
            logger.info(f"No data needs needs to be optimized, latest operation was: {operation}")
            
    except Exception as e:
        logger.error(f"Failed to optimize the data under path {path}")
        raise e
    


# # Unittesting

# In[212]:


run_tests = False
logger = sc._jvm.org.apache.log4j.LogManager.getLogger("DDP - TESTING")


# ## test write function for full load

# In[213]:


import unittest
import pandas as pd
import numpy as np
import pandas.testing as pd_testing
import warnings


class TestDDPLib(unittest.TestCase):

    @classmethod
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

        #initialise logger
        #self.logger = sc._jvm.org.apache.log4j.LogManager.getLogger("DDP - TESTING")

        # get the current storage account name
        self.storage_account = get_storage_name()
        
        spark.conf.set("spark.sql.execution.arrow.enabled", "false")
        
        self.test_data = [
            (
                pd.DataFrame({
                    "col1": [np.random.randint(10) for x in range(10)],
                    "col2": [np.random.randint(100) for x in range(10)]})
                , "v_001"),

            (
                pd.DataFrame({
                    "col1": [np.random.randint(10) for x in range(10)],
                    "col2": [np.random.randint(100) for x in range(10)],
                    "col3": [np.random.randint(100) for x in range(10)],})
                , "v_002"),
            (
                pd.DataFrame({
                    "col2": [np.random.randint(100) for x in range(10)],
                    "col3": [np.random.randint(100) for x in range(10)],})
                , "v_002")
        ]
    
    #@classmethod
    #def tearDown(self):
    #    dbutils.fs.rm("abfss://ddp-structured-test@zc01xyumasynstc5fanp00.dfs.core.windows.net/test_full_load/", recurse=True)

    def test_write_full_load(self):

        for test_case in self.test_data:
            test = spark.createDataFrame(test_case[0])
            
            # execute the full write function with full load configuration
            # the dataframe for the test cases are defined in the setUp function
            ddp_lib_write(
                df=test,
                write_mode="overwrite", 
                storage_account=self.storage_account, 
                container_name="ddp-structured-test",
                blob_path="test_full_load",
                load_type="full",
                audit_container_name="ddp-structured-test",
                audit_storage_account=self.storage_account,
                audit_log_path="audit",
                load_date=datetime.now(),
                primary_keys=None)

            df_res = spark.read.format("delta").load("abfss://ddp-structured-test@zc01xyumasynstc5fanp00.dfs.core.windows.net/test_full_load/{}/".format(test_case[1]))
        
            assert sorted(test.collect()), sorted(df_res.collect())


if run_tests:
    unittest.main(TestDDPLib(), argv=[''], verbosity=2, exit=False)


