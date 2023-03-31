import datetime
import logging
import snowflake.connector
import boto3
import json
from fastparquet import ParquetFile
import io
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import pandas as pd
from io import BytesIO
import pyarrow.parquet as pq
import boto3
from io import StringIO
from typing import Dict, List
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    

    credential = DefaultAzureCredential()
    vault_url = "https://blobtosnowflakekeyvault.vault.azure.net/"
    client = SecretClient(vault_url=vault_url, credential=credential)

    account = client.get_secret('snowflakeaccountname').value
    user = client.get_secret('snowflakeusername').value
    password = client.get_secret('snowflakepassword').value
    snowflake_database = client.get_secret("snowflakedatabase").value
    schema_name = client.get_secret("schemaname").value
    stage_name  = client.get_secret("stagename").value
    containername = client.get_secret("containername").value
    snowflakerole = client.get_secret("snowflakerole").value
    snowflakewarehouse = client.get_secret("snowflakewarehouse").value


    azureblobstorageurl = client.get_secret("azureblobstorageurl").value
    azuresastoken = client.get_secret("azuresastoken").value
    connect_str = client.get_secret("blobconnectionstring").value

    conn = snowflake.connector.connect(
        account=account,warehouse = snowflakewarehouse, user=user, password=password, database=snowflake_database, schema=schema_name, role=snowflakerole
    )

    cur = conn.cursor()
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(containername)


    # List the blobs in the container
    blob_list = container_client.list_blobs()
    l = [blob.name for blob in BlobServiceClient.from_connection_string(connect_str).get_container_client(containername).list_blobs()]
    mapping = cluster_by_database_and_table(l)
    print(mapping.items())


    cur.execute(
            f"""CREATE STAGE IF NOT EXISTS {snowflake_database}.{schema_name}.{stage_name} 
            URL= '{azureblobstorageurl}'
            CREDENTIALS = (
            AZURE_SAS_TOKEN = '{azuresastoken}'
            );
            """
        )


    for database, tables_list in mapping.items():
        for table, latest_file in tables_list.items():
            # Download the latest file from Blob Storage
            blob_client = container_client.get_blob_client(blob=latest_file.replace("blob://", ""))
            stream_downloader = blob_client.download_blob()
            stream = BytesIO()
            stream_downloader.readinto(stream)
            
            # Read the Parquet table using PyArrow
            parquet_table = pq.read_table(stream)
            
            # Create a dictionary of column names and their types
            column_dict = {parquet_table.schema[i].name: 'STRING' for i in range(parquet_table.num_columns)}


            # count_ddl = f"""select COLUMN_NAME from {snowflake_database}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table.upper()}'"""
            # cursor = conn.cursor()
            # snowflake_column_list = cursor.execute(count_ddl).fetchall()
            # updated_column_list = [item[0] for item in snowflake_column_list] 
            
            # #Code to Alter the table and add new columns
            # if(len(updated_column_list) != 0):
            #     if(len(updated_column_list) != len(column_dict)):
            #         s3_table_column_list = list(sorted(set(list(column_dict.keys())) - set(updated_column_list)))
            #         for column in enumerate(s3_table_column_list):
            #             alter_ddl = f"""alter table {table.upper()} add column {column} STRING"""
            #             cur.execute(alter_ddl)
            #             print(f'new column {column} added to {table}')
            
            # Create the SQL statement for creating the table
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {snowflake_database}.{schema_name}.{table} ("
            for column_name, column_type in column_dict.items():
                create_table_sql += f"{column_name} {column_type},"
            create_table_sql = create_table_sql[:-1]  # Remove the trailing comma
            create_table_sql += ")"

            # Create the table in Snowflake
            cur.execute(create_table_sql)

            # Load data from the Parquet file into Snowflake
            latest_blob_name = latest_file.replace("blob://", "")
            pipe_name = f"{table}_pipe"
            copy_into_sql = f"CREATE OR REPLACE PIPE {snowflake_database}.{schema_name}.{pipe_name} AS "
            copy_into_sql += f"COPY INTO {table} FROM (SELECT {', '.join([f'$1:{key}' for key in column_dict.keys()])}"
            copy_into_sql += f" FROM @{stage_name}/{latest_blob_name}) FILE_FORMAT=(TYPE='PARQUET')"
            
            # Execute the COPY INTO statement using Snowflake connector
            cur.execute(copy_into_sql)

            # Resume and refresh the pipe
            cur.execute(f'ALTER PIPE {snowflake_database}.{schema_name}.{pipe_name} SET PIPE_EXECUTION_PAUSED=false')
            cur.execute(f'ALTER PIPE {snowflake_database}.{schema_name}.{pipe_name} REFRESH')
            print(f'Done creating assets for {table}')

    cur.close()
    conn.close()


def cluster_by_database_and_table(file_list: List[str]) -> Dict[str, Dict[str, str]]:
    """
    Clusters file paths by database and table, and returns the most recent file path for each grouping.

    Args:
        file_list (List[str]): A list of file paths in the format of 'database/table/partition/file'.

    Returns:
        Dict[str, Dict[str, str]]: A dictionary containing the most recent file path for each database and table grouping.
    """
    clusters = {}
    for path in file_list:
        parts = path.split("/")
        if len(parts) < 3:
            continue
        db = parts[0]
        table = parts[1]
        if db not in clusters:
            clusters[db] = {}
        if table not in clusters[db]:
            clusters[db][table] = path
        else:
            existing_path = clusters[db][table]
            existing_date = existing_path.split("/")[-1].split("_")[1]
            new_date = path.split("/")[-1].split("_")[1]
            if new_date > existing_date:
                clusters[db][table] = path

    # Add the "blob://" prefix to the file paths and return the dictionary
    for db in clusters:
        for table in clusters[db]:
            clusters[db][table] = "blob://" + clusters[db][table]

    return clusters
