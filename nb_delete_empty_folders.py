# Databricks notebook source
# MAGIC %md
# MAGIC ### nb_delete_empty_folders
# MAGIC  Using this notebook, you can delete all empty folders in a given container and folder in your current storage account.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import expr
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import when
from pyspark.sql.functions import lit
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import rtrim

from azure.storage.blob import BlobServiceClient


# COMMAND ----------

# MAGIC %run ./nb_ddp_lib

# COMMAND ----------

# get storage account name
storage_account_name = get_storage_name()


# Set input parameters
p_container_name = "ddp-transient"
p_folder_name = "pi-odbc/test"

# COMMAND ----------

def dir_cleaner(storage_account_name, container_name, folder_name):
    """This recursive function traverses a given folder and removes empty folders from the container.
    Parameters:
        storage_account_name(str): The storage account in them container exists.
        container_name(str): the container in them folder exists.
        folder_name(str): the folder that should clean.
    Returns:
        "true"/"false": it returns recursively "true" or "false" for each visited folder/subfolder, if "true" that means folder is empty and then deletes it.
    
    """
    folder_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_name}"
    subfolder_paths = [f.path for f in dbutils.fs.ls(folder_path) if f.isDir]
    file_paths = [f.path for f in dbutils.fs.ls(folder_path) if not f.isDir]

    for subfolder_path in subfolder_paths:
        subfolder_name = subfolder_path[len(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"):]
        is_empty = dir_cleaner(storage_account_name, container_name, subfolder_name)
        if is_empty:
            dbutils.fs.rm(subfolder_path, recurse=True)
            print(f"Deleted empty folder '{subfolder_path}'")

    subfolder_paths = [f.path for f in dbutils.fs.ls(folder_path) if f.isDir]
    file_paths = [f.path for f in dbutils.fs.ls(folder_path) if not f.isDir]

    if len(subfolder_paths) == 0 and len(file_paths) == 0:
        return True
    else:
        return False


# In[183]:


dir_cleaner(storage_account_name=storage_account_name, container_name=p_container_name, folder_name=p_folder_name)
