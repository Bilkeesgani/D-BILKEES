import json
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import IntegrityError
import csv
import shutil
import tempfile
import boto3
import botocore
import concurrent.futures
import snowflake.connector
## availalble in standard module ##
import urllib3
import os
import sys
import time
from datetime import datetime
from datetime import timedelta,date
from io import StringIO
os.sys.path.append('/opt')

from dateutil import tz
from dateutil.parser import parse
import urllib.parse
import great_expectations as ge
import redshift_connector
from snowflake.connector.pandas_tools import write_pandas
import awswrangler as wr
import io
import re
import ast

etl_log_table = 'app.etl_config_log'
etl_config_table = 'app.etl_config'
error_table_schema = "err"


# Class for creating Connections
class ConnectionMaker:
    def __init__(self, conn_type=None, **kwargs):
        if conn_type is None:
            raise ValueError("Connection type is required.")
        
        self.conn_type = conn_type.lower()
        
        try:
            if self.conn_type in ['postgres', 'rds']:
                self.engine = psycopg2
            elif self.conn_type == 'redshift':
                self.engine = redshift_connector
            elif self.conn_type == 'snowflake':
                self.engine = snowflake.connector
            elif self.conn_type == 's3':
                self.engine = boto3.client('s3')
            elif self.conn_type == 'filesystem':
                try:
                    self.engine = kwargs.get('file_path')
                except Exception as e:
                    print(e)
                    raise Exception(e)
            else:
                raise ValueError("Unsupported conn type.")
        except Exception as e:
            print(e)
            raise Exception(e)
        
    # block for returning connection
    def get_engine(self):
        return self.engine  

    # block for getting file from source
    def get_file(self, **kwargs):
        if self.conn_type == 's3':
            obj = self.engine.get_object(Bucket=kwargs.get('bucket'), Key=kwargs.get('key'))
            if kwargs.get("read_size"):
                read_size = kwargs.get("read_size")
                return obj['Body'].read(read_size).decode('utf-8')
            else:
                return obj['Body'].read().decode('utf-8')
        elif self.conn_type == 'filesystem':
            if self.engine:
                with open(self.engine, 'r') as file:
                    return file.read()
            else:
                raise ValueError("File path is required for filesystem source type.")
        else:
            raise ValueError("Unsupported operation for this conn type.")

    # block for getting file from source
    def put_file(self, **kwargs):
        if self.conn_type == 's3':
            key = kwargs.get('key')
            data = kwargs.get('data')
            if key and data:
                self.engine.put_object(Bucket=kwargs.get('bucket'), Key=kwargs.get('key'), Body=kwargs.get('data'))
            else:
                raise ValueError("key and data are required for S3 destination type.")
        elif self.conn_type == 'filesystem':
            file_path = kwargs.get('file_path')
            data = kwargs.get('data')
            if file_path and data:
                with open(file_path, 'w') as file:
                    file.write(data)
            else:
                raise ValueError("File path and data are required for filesystem destination type.")
        else:
            raise ValueError("Unsupported operation for this conn type.")        
        

# Function for logging etl load info to etl_config_log table
def log_etl_load_info(engine, config_id, conn_params, log_id,  **kwargs):
    parameters = {}
    update_query = f"""UPDATE {etl_log_table} SET {', '.join([f'{key} = %s' for key in kwargs.keys()])} 
    WHERE config_id = {config_id} AND log_id ={log_id} """
    parameters.update(kwargs)

    try:
        with engine.connect(**conn_params) as conn:
            cur = conn.cursor()
            cur.execute(update_query, tuple(parameters.values()))
            conn.commit()
            cur.close()
    except Exception as e:
        print(e)


# Function to generate table DDL
def generate_table_ddl(table_schema, table_name, df):
    """This Functions Generates Table DDL for Creating Tables, column names are taken from the dataframe"""
    ddl_statements = []
    ddl_statements.append(f"CREATE TABLE {table_schema}.{table_name} (")
    # Convert all column names to strings
    for column_name in df.columns:
        ddl_statement = f'"{column_name}" VARCHAR(max),'
        ddl_statements.append(ddl_statement)
    ddl_statements[-1] = ddl_statements[-1][:-1] 
    ddl_statements.append(");")
    return "\n".join(ddl_statements)


# Function for checking columns 
def pre_process_column_mapping(raw_table_mapping, df):
    try:
        source_columns = [col['src_col_name'] for col in raw_table_mapping]
        missing_columns = list(set(source_columns) - set(df.columns))
        missing_required_columns = []
        for col in raw_table_mapping:
            if col['src_col_name'] in missing_columns:
                if col['required'].lower() == 'yes':
                    missing_required_columns.append(col['src_col_name'])
                

        extra_columns = list(set(df.columns) - set(source_columns))

        return missing_columns, extra_columns, missing_required_columns
    except Exception as e:
        print(e)
        raise Exception (e)    


# Function For loading data into staging table
def load_raw_table(conn_type, engine, raw_table_schema, raw_table_name, b_name, key, delimiter, quote_char, df, region, redshift_role, raw_table_mapping, raw_table_created, has_header, copy_cmd_type,  **kwargs):
    
    raw_table_schema = raw_table_schema.strip()
    raw_table_name = raw_table_name.strip()
    try:
        for mapping in raw_table_mapping:
            source_col = mapping["src_col_name"]
            target_col = mapping["raw_col_name"]
            df.rename(columns={source_col: target_col}, inplace=True)

        df_columns = df.columns
        column_str = '", "'.join(df_columns)
        column_str = '"{0}"'.format(column_str)
        
        if not raw_table_created:
            raw_table_creation_status = False
            table_ddl = generate_table_ddl(raw_table_schema, raw_table_name, df)
            # print("table ddl")
            # print(table_ddl)

            with engine.connect(**kwargs) as conn:
                cur = conn.cursor()
                sql = f'DROP TABLE IF EXISTS {raw_table_schema}.{raw_table_name}'
                try:
                    cur.execute(sql)
                    conn.commit()
                    print(f'RAW Table {raw_table_schema}.{raw_table_name} DROPPED')
                except Exception as e:
                    print(e)
                    raise Exception(e)
                sql = table_ddl
                try:
                    cur.execute(sql)
                    conn.commit()
                    cur.close()
                    raw_table_creation_status = True
                    print(f"RAW Table {raw_table_schema}.{raw_table_name} CREATED")
                except Exception as e:
                    print(e)
                    raise Exception(e)
        else:
            raw_table_creation_status = True

        with engine.connect(**kwargs) as conn:
            cur = conn.cursor()
            if conn_type.lower() == 'snowflake':
                pass
                #implement logic for directly copying from s3

            elif conn_type.lower() == 'redshift':
                if has_header:
                    print(f"trying simple no format copy command raw table load: {raw_table_name}") # default to simple copy command working for most of the tables
                    sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                        FROM 's3://{b_name}/{key}'
                        IAM_ROLE '{redshift_role}'
                        DELIMITER '{delimiter}'
                        NULL AS ''
                        ACCEPTINVCHARS '^'
                        IGNOREBLANKLINES
                        TRUNCATECOLUMNS
                        IGNOREHEADER 1;"""
                else:
                    sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                        FROM 's3://{b_name}/{key}'
                        IAM_ROLE '{redshift_role}'
                        DELIMITER '{delimiter}'
                        NULL AS ''
                        ACCEPTINVCHARS '^'
                        IGNOREBLANKLINES
                        TRUNCATECOLUMNS;"""
                print("sql\n", sql)
                try:
                    cur.execute(sql)
                    conn.commit()
                    cur.close()
                    print("raw table copied")
                except Exception as e:
                    conn.rollback()
                    print(e)
                    print("Could Not copy raw table with no format command quote")
                    print(f"trying copy command with csv quote for table: {raw_table_name}")
                    if has_header:
                        sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                            FROM 's3://{b_name}/{key}'
                            IAM_ROLE '{redshift_role}'
                            CSV
                            QUOTE AS '{quote_char}'
                            DELIMITER '{delimiter}'
                            NULL AS ''
                            ACCEPTINVCHARS '^'
                            IGNOREBLANKLINES
                            TRUNCATECOLUMNS
                            IGNOREHEADER 1;"""

                    else:
                        sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                            FROM 's3://{b_name}/{key}'
                            IAM_ROLE '{redshift_role}'
                            CSV
                            QUOTE AS '{quote_char}'
                            DELIMITER '{delimiter}'
                            NULL AS ''
                            ACCEPTINVCHARS '^'
                            TRUNCATECOLUMNS
                            IGNOREBLANKLINES;"""
                    print('sql\n',sql)
                    try:
                        cur.execute(sql)
                        conn.commit()
                        cur.close()
                        print("raw table copied")
                    except Exception as e:
                        conn.rollback()
                        print(e)
                        print("Could Not copy raw table with csv quote")
                        print(f"Trying copy command without remove quotes and escape for raw table: {raw_table_name}")
                        if has_header:
                            sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                                FROM 's3://{b_name}/{key}'
                                IAM_ROLE '{redshift_role}'
                                DELIMITER '{delimiter}'
                                ESCAPE
                                NULL AS ''
                                ACCEPTINVCHARS '^'
                                IGNOREBLANKLINES
                                TRUNCATECOLUMNS
                                IGNOREHEADER 1;"""

                        else:
                            sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                                FROM 's3://{b_name}/{key}'
                                IAM_ROLE '{redshift_role}'
                                DELIMITER '{delimiter}'
                                ESCAPE
                                NULL AS ''
                                ACCEPTINVCHARS '^'
                                TRUNCATECOLUMNS
                                IGNOREBLANKLINES;"""
                        print('sql\n',sql)
                        try:
                            cur.execute(sql)
                            conn.commit()
                            cur.close()
                            print("raw table copied")
                        except Exception as e:
                            conn.rollback()
                            print(e)
                            print("Could Not copy raw table with  escape option")
                            print(f"Trying copy command with remove quotes  escape only for raw table: {raw_table_name}")
                            if has_header:
                                sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                                        FROM 's3://{b_name}/{key}'
                                        IAM_ROLE '{redshift_role}'
                                        DELIMITER '{delimiter}'
                                        REMOVEQUOTES
                                        ESCAPE
                                        NULL AS ''
                                        ACCEPTINVCHARS '^'
                                        IGNOREBLANKLINES
                                        TRUNCATECOLUMNS
                                        IGNOREHEADER 1;"""

                            else:
                                sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                                        FROM 's3://{b_name}/{key}'
                                        IAM_ROLE '{redshift_role}'
                                        DELIMITER '{delimiter}'
                                        REMOVEQUOTES
                                        ESCAPE
                                        NULL AS ''
                                        ACCEPTINVCHARS '^'
                                        TRUNCATECOLUMNS
                                        IGNOREBLANKLINES;"""
                            print('sql\n', sql)
                            try: 
                                cur.execute(sql)
                                conn.commit()
                                cur.close()
                                print("raw table copied")
                            except Exception as e:
                                conn.rollback()
                                print(e)
                                print("could not copy with remove quotes escape option")
                                print("trying copy with removequotes only")
                                if has_header:
                                    sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                                        FROM 's3://{b_name}/{key}'
                                        IAM_ROLE '{redshift_role}'
                                        DELIMITER '{delimiter}'
                                        REMOVEQUOTES
                                        NULL AS ''
                                        ACCEPTINVCHARS '^'
                                        IGNOREBLANKLINES
                                        TRUNCATECOLUMNS
                                        IGNOREHEADER 1;"""

                                else:
                                    sql = f"""COPY {raw_table_schema}.{raw_table_name} ({column_str})
                                        FROM 's3://{b_name}/{key}'
                                        IAM_ROLE '{redshift_role}'
                                        DELIMITER '{delimiter}'
                                        REMOVEQUOTES
                                        NULL AS ''
                                        ACCEPTINVCHARS '^'
                                        TRUNCATECOLUMNS
                                        IGNOREBLANKLINES;"""
                                print('sql\n',sql)
                                try:
                                    cur.execute(sql)
                                    conn.commit()
                                    cur.close()
                                    print("raw table copied")
                                except Exception as e:
                                    print(e)                            
                                    raise Exception(e)
                                

            elif conn_type.lower() == 'rds':
                if has_header:
                    sql = f"""SELECT aws_s3.table_import_from_s3(
                        '{raw_table_schema}.{raw_table_name}',
                        '',
                        'DELIMITER ''{delimiter}'' CSV HEADER QUOTE ''{quote_char}''',
                        aws_commons.create_s3_uri('{b_name}', '{key}', '{region}')

                    );"""
                else:
                    sql = f"""SELECT aws_s3.table_import_from_s3(
                    '{raw_table_schema}.{raw_table_name}',
                    '',
                    'DELIMITER ''{delimiter}'' CSV QUOTE ''{quote_char}''',
                    aws_commons.create_s3_uri('{b_name}', '{key}', '{region}')

                );"""
                try:
                    cur.execute(sql)
                    conn.commit()
                    cur.close()
                    print("raw table copied")
                except Exception as e:
                    print(e)
                    raise Exception(e)

        return raw_table_creation_status
    except Exception as e:
        print(e)
        raise Exception(e)


# Function for loading data from raw table into target table   
def load_target_table(target_table_type, target_table_engine, target_table_mapping, target_table_schema, target_table_name, expectation_dict, data_date, etl_id, now, req_id, primary_key_cols, target_con_params, delimiter, quote_char, redshift_role, env_prefix, has_header, b_name, key, missing_columns, chunk_size, load_type):
    
    target_table_schema = target_table_schema.strip()
    target_table_name = target_table_name.strip()
    
    chunk_size = chunk_size
    dirty_df_list = [] # Empty List to store dirty df dataframes
    # truncate existing table for alpha load
    if (load_type=="alpha"):
        sql_truncate =  f'''
            Truncate table {target_table_schema}.{target_table_name} '''
        with target_table_engine.connect(**target_con_params) as conn:
            try:
                cur = conn.cursor()
                cur.execute(sql_truncate)
                conn.commit()
                #cur.close()
            except Exception as e:
                print(e)
                cur.close()
            print('Table {0} truncated sucessfully'.format(target_table_name))

    for df in wr.s3.read_csv(f's3://{b_name}/{key}', delimiter=delimiter, quotechar=quote_char, chunksize=chunk_size, header=None if not has_header else 0, dtype='object', encoding_errors = 'ignore', keep_default_na=False, na_values=[""]):
        # Reset index to start from 0 for each chunk
        df.reset_index(drop=True, inplace=True)
        
        if missing_columns: # Add null data in missing columns if any
            for column_name in missing_columns:
                df[column_name]=np.nan
        
        for mapping in target_table_mapping:
            source_col = mapping["source_col_name"]
            target_col = mapping["target_col_name"]
            df.rename(columns={source_col: target_col}, inplace=True)

        columns_to_select = [mapping["target_col_name"] for mapping in target_table_mapping]
        df = df[columns_to_select]

        df['datadate'] = data_date
        df['run_id'] = req_id
        df['run_ts'] = f"{now}"
        df['etl_id'] = etl_id

        # Start Data Quality checking  
        dirty_df = None 
        if expectation_dict and len(primary_key_cols) !=0:
            #ge_df = ge.from_pandas(df)
            expectation_suite = ge.core.ExpectationSuite(**expectation_dict)
            result = ge.from_pandas(df).validate(expectation_suite, result_format="COMPLETE")
            unexpected_indexes = []
            checks_failed = {}  # Dictionary to store check types failed for each failed row
            if result.results:
                for i in range(len(result.results)):
                    if not result.results[i].get("success", True):
                        unexpected_indexes.extend(result.results[i].result.get("unexpected_index_list", []))
                        check_failed = result.results[i].get("expectation_config", "Unknown").get("meta", "Unknown").get("CheckComment", "Unknown")
                        for idx in result.results[i].result.get("unexpected_index_list", []):
                            if idx not in checks_failed:
                                checks_failed[idx] = []
                            checks_failed[idx].append(check_failed)
            unexpected_indexes = list(set(unexpected_indexes))
            if len(unexpected_indexes) != 0:
                dirty_df = df.iloc[unexpected_indexes]
                dirty_df["checks_failed"] = dirty_df.index.map(checks_failed).str.join(', ') 
                df = df.drop(index=unexpected_indexes)
        


        dtype_mapping = {}
        int_type_mapping = {}

        for mapping in target_table_mapping:
            target_col = mapping["target_col_name"]
            target_dtype = mapping["target_col_datatype"]

            # Logic to determine the target data type for each column
            if target_dtype.lower() in ["float", "double"]:
                dtype_mapping[target_col] = "Float64"
            elif target_dtype.lower() == "integer":
                dtype_mapping[target_col] = "Float64" 
                int_type_mapping[target_col] = "Int64"
            elif target_dtype.lower() in ['datetime', 'timestamp', 'timestamp with timezone']:
                # dtype_mapping[target_col] = "datetime64[ns]"
                try:
                    df[target_col] = pd.to_datetime(df[target_col], format='mixed')
                except (pd.errors.OutOfBoundsDatetime) as e:
                    print("Caught OutOfBoundsDatetime exception:", e)
                    # Remove 'T' and 'Z' if present, and ensure correct length
                    def clean_date_string(date_str):
                        if pd.isna(date_str):  # Handle NaN values
                            return pd.NaT
                        clean_str = re.sub(r'[TZ]', ' ', date_str).strip()
                        return clean_str if len(clean_str) >= 10 else pd.NaT  # Return None for invalid formats
        
                    df[target_col] = df[target_col].apply(clean_date_string)
                    # Try converting again after cleaning
                    df[target_col] = pd.to_datetime(df[target_col], format='mixed', errors='ignore')
                except (ValueError) as e:
                    print(e)
                    raise ValueError(e)
            elif target_dtype.lower() == 'date':
                try:
                    df[target_col] = pd.to_datetime(df[target_col], format = 'mixed').dt.date
                except (pd.errors.OutOfBoundsDatetime) as e:
                    print("Caught OutOfBoundsDatetime exception:", e)
                    
                    def handle_out_of_bounds_date(value):
                        if pd.isna(value):  # Handle NaN values
                            return pd.NaT
                        try:
                            return parse(value).date()
                        except Exception:
                            print("Could not parse:", value)
                            return str(value)  


                    df[target_col] = df[target_col].apply(handle_out_of_bounds_date)
                except (ValueError) as e:
                    print(e)
                    raise ValueError(e)
            elif target_dtype.lower() == "boolean" and df[target_col].dtype == 'object':
                try:
                    true_values = {'true', 'yes', '1'}
                    false_values = {'false', 'no', '0'}
                    mapping_dict = {**{val: True for val in true_values}, **{val: False for val in false_values}}
                    df[target_col] = df[target_col].str.lower().map(mapping_dict).fillna(np.nan)
                except Exception as e:
                    print(e)
                    raise Exception(e)

        # Convert the entire DataFrame
        try: 
            df = df.astype(dtype_mapping, errors='raise')
        except Exception as e:
            print("float type conversion failed for dataframe")
            print(e)
            raise Exception(e)

        # Convert the intermediate float type to integer for integerr columns
        try: 
            df = df.astype(int_type_mapping, errors='raise')
        except Exception as e:
            print(e)
            raise Exception(e)

                                
                
        if target_table_type.lower() == 'snowflake':
            primary_key_cols = [col.upper() for col in primary_key_cols]
            with target_table_engine.connect(**target_con_params) as conn:
                cur = conn.cursor()
                df.columns = df.columns.str.upper()
                f'{target_table_name}.csv'
                file_path = f'{target_table_name}.csv'
                column_list = df.columns
                df.to_csv(file_path, index=False, sep=delimiter, lineterminator='\n')
                sql = f"USE SCHEMA {target_table_schema}"
                try:
                    cur.execute(sql)
                except Exception as e:
                    print(e)
                    cur.close()
                    raise Exception(e)
                sql = f'''
                        CREATE TEMPORARY TABLE temp_{target_table_name} LIKE {target_table_name}
                    '''
                try:
                    cur.execute(sql)
                    print("temp table created")
                except Exception as e:
                    print(e)
                    raise Exception(e)
                try:
                    file_path = f'{target_table_name}.csv'
                    cur.execute("PUT file://" + file_path + " @DM")
                except Exception as e:
                    print(e)
                    cur.close()
                    raise Exception(e)
                sql = f"""COPY INTO temp_{target_table_name}
                        FROM @DM/{target_table_name}.csv
                        FILE_FORMAT = (FORMAT_NAME = csv_format FIELD_DELIMITER = {delimiter} FIELD_OPTIONALLY_ENCLOSED_BY = '{quote_char}'
                        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"""
                
                try:
                    cur.execute(sql)
                    os.remove(file_path)
                    df = None
                except Exception as e:
                    print(e)
                    cur.close()
                    raise Exception(e)
                sql = f"remove @dm/{target_table_name}.csv"
                try:
                    cur.execute(sql)
                except Exception as e:
                    print(e)
                    
                # Construct on Clause dynamically on top of primary keys to determine incremental load
                on_clause = ' AND '.join(f'target.{col} = source.{col}' for col in primary_key_cols)

                # Construct SET clause dynamically to determine which columns in target table need update from the incoming data
                cols_to_update = ', '.join(f'{col} = source.{col}' for col in column_list)
                # Construct column string (names of columns of target table) from column list
                column_str = ", ".join(column_list)
                # Construct Value string (names of source columns to load dat from) into columns of target table
                col_values = ", ".join(f"source.{col}" for col in column_list)
                sql = f'''
                    MERGE INTO {target_table_name} AS target
                    USING temp_{target_table_name} AS source
                    ON {on_clause}
                    WHEN MATCHED THEN
                    UPDATE SET {cols_to_update}
                    WHEN NOT MATCHED THEN
                    INSERT ({column_str})
                    VALUES ({col_values})
                    '''
                try:
                    cur.execute(sql)
                    conn.commit()
                except Exception as e:
                    print(e)
                    raise Exception(e)
                cur.close()

        elif target_table_type.lower() == 'redshift':
            with target_table_engine.connect(**target_con_params) as conn:
                cur = conn.cursor()
                column_list = df.columns
                column_str = '", "'.join(column_list)
                column_str = '"{0}"'.format(column_str)
                s3 = boto3.client('s3')
                # bucket name for acting as temporary stage for redshift#
                bucket_name = f'{env_prefix}-datalake-s3-land' 
                file_name = f'{target_table_name}.csv'
                file_key = f'redshift_stage/{file_name}' 
                csv_str = df.to_csv(index=False, sep=f'{delimiter}', quotechar=f'{quote_char}')
                try:
                    s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_str)
                    csv_str = None
                except Exception as e:
                    print("Error uploading CSV file to S3:", e)
                    raise Exception(e)
                if (load_type=="reload"):
                    sql_text =  f'''
                        Delete from {target_table_schema}.{target_table_name}  where '''
                    try:
                        
                        for col in primary_key_cols:
                            Keys = ''
                            for value in df[col]:
                                Keys = Keys + "'{0}'".format(value) + ','
                            Keys = Keys[:-1]     
                            delete_clause =  ' {0} in ({1}) and'.format(col,Keys) 
                        delete_clause = delete_clause.replace(") and",")")
                        sql_text = sql_text + delete_clause
                        print(sql_text)
                        try:
                            cur.execute(sql_text)
                            conn.commit()
                            #cur.close()
                        except Exception as e:
                            print(e)
                            print('Table {0} deletes Failed'.format(target_table_name))
                            #cur.close()
                        print('Table {0} deletes performed sucessfully'.format(target_table_name))

                        sql = f"""
                            COPY {target_table_schema}.{target_table_name} ({column_str})
                            FROM 's3://{bucket_name}/{file_key}'
                            IAM_ROLE '{redshift_role}'
                            CSV
                            DELIMITER '{delimiter}'
                            QUOTE '{quote_char}'
                            NULL AS ''
                            ACCEPTINVCHARS '^'
                            ACCEPTANYDATE
                            IGNOREBLANKLINES 
                            IGNOREHEADER 1
                            ROUNDEC
                            COMPUPDATE OFF STATUPDATE OFF;
                            """
                        try:
                            cur.execute(sql)
                            conn.commit()
                            df = None
                        except Exception as e:
                            print(e)
                            raise Exception(e)
                    except Exception as e:
                        print(e)
                        cur.close()
                        raise Exception(e)
                else:
                    if len(primary_key_cols) !=0: # Dedupe Based On Primary Keys
                        sql = f'''
                        CREATE TEMPORARY TABLE temp_{target_table_name} (LIKE {target_table_schema}.{target_table_name})
                        '''
                        try:
                            cur.execute(sql)
                        except Exception as e:
                            print(e)
                            raise Exception(e)
                        sql = f"""
                            COPY temp_{target_table_name} ({column_str})
                            FROM 's3://{bucket_name}/{file_key}'
                            IAM_ROLE '{redshift_role}'
                            CSV
                            DELIMITER '{delimiter}'
                            QUOTE '{quote_char}'
                            NULL AS ''
                            ACCEPTINVCHARS '^'
                            ACCEPTANYDATE
                            IGNOREBLANKLINES 
                            IGNOREHEADER 1
                            ROUNDEC
                            COMPUPDATE OFF STATUPDATE OFF;
                            """
                        try:
                            cur.execute(sql)
                            conn.commit()
                            df = None
                            # # Delete file from S3 after successful copy to Redshift
                            # s3.delete_object(Bucket=bucket_name, Key=file_key)
                            # print("File deleted from S3.")
                        except Exception as e:
                            print(e)
                            raise Exception(e)
                        on_clause = ' AND '.join(f'{target_table_schema}.{target_table_name}.{col} = source.{col}' for col in primary_key_cols)

                        
                        # # Construct SET clause dynamically to determine which columns in target table need update from the incoming data
                        cols_to_update = ', '.join(f'{col} = source.{col}' for col in column_list)
                        # Construct Value string (names of source columns to load dat from) into columns of target table
                        col_values =", ".join(f"source.{col}" for col in column_list)
                        sql = f'''
                            MERGE INTO {target_table_schema}.{target_table_name}
                            USING temp_{target_table_name} AS source
                            ON {on_clause}
                            REMOVE DUPLICATES'''
                        try:
                            cur.execute(sql)
                            conn.commit()
                            cur.close()
                        except Exception as e:
                            msg = "Failed to exceute Merge Statement on Redshift"
                            print_msg = msg + '\n\r' + str(e)
                            print(print_msg)
                            cur.close()
                            raise Exception(print_msg)
                    else: # No dedupe Needed for tables without primary keys
                        sql = f"""
                            COPY {target_table_schema}.{target_table_name} ({column_str})
                            FROM 's3://{bucket_name}/{file_key}'
                            IAM_ROLE '{redshift_role}'
                            CSV
                            DELIMITER '{delimiter}'
                            QUOTE '{quote_char}'
                            NULL AS ''
                            ACCEPTINVCHARS '^'
                            ACCEPTANYDATE
                            IGNOREBLANKLINES 
                            IGNOREHEADER 1
                            COMPUPDATE OFF STATUPDATE OFF;
                            """
                        try:
                            cur.execute(sql)
                            conn.commit()
                            df = None
                            # # Delete file from S3 after successful copy to Redshift
                            # s3.delete_object(Bucket=bucket_name, Key=file_key)
                            # print("File deleted from S3.")
                        except Exception as e:
                            print(e)
                            raise Exception(e)



        elif target_table_type.lower() in ['rds', 'postgres']:     
            with target_table_engine.connect(**target_con_params) as conn:
                cur = conn.cursor()
                column_list = df.columns
                column_str = ', '.join(column_list)
                sio = io.StringIO()
                df.to_csv(sio, index=None, sep=f'{delimiter}', quotechar=f'{quote_char}')
                sio.seek(0)
                copy_sql = f"COPY {target_table_schema}.{target_table_name} ({column_str}) FROM STDIN DELIMITER '{delimiter}' CSV HEADER QUOTE '{quote_char}';"
                try:  
                    cur.copy_expert(copy_sql, file=sio)
                    conn.commit()
                    sio.close()
                    df = None
                except IntegrityError as e:
                    conn.rollback()
                    print("Integrity error occurred during COPY operation:", e)
                    try:
                        print("Attempting bulk insert with ON CONFLICT DO UPDATE.")
                        sio.seek(0)
                        reader = csv.reader(sio, delimiter= delimiter)
                        header_row = next(reader)
                        print(f"header_row: {header_row}")
                        print(type(header_row))
                        columns = ', '.join(header_row)
                        rows = []
                        # Clean the rows to replace blank values with NULL values
                        for row in reader:
                            processed_row = [None if value == '' else value for value in row]
                            rows.append(processed_row)
                        sio.close()
                        df = None
                    
                        primary_key_cols_str = ', '.join(col for col in primary_key_cols)
                        col_update_str = ", ".join([f"{col} = excluded.{col} " for col in header_row])

                        sql = f"INSERT INTO {target_table_schema}.{target_table_name} ({columns}) VALUES %s ON CONFLICT ({primary_key_cols_str}) DO UPDATE SET {col_update_str};"
                        execute_values(cur, sql, rows)
                        conn.commit()
                    except Exception as e:
                        print(e)
                        raise Exception(e)
                finally:
                    cur.close()
        
        
        if dirty_df is not None:
            dirty_df_list.append(dirty_df)

    return dirty_df_list

            
# Save Dirty records to error table
def save_error_records_to_table(df, target_table_engine,  target_table_type, target_table_schema, target_table_name, data_date, target_con_params, redshift_role, delimiter, quote_char, etl_id, env_prefix):

    target_table_schema = target_table_schema.strip()
    target_table_name = target_table_name.strip()
    
    error_table_created = False
    data_date = data_date.replace("-", "")
    table_name = f"{target_table_name}_error_{data_date}_{etl_id}"
    sql = generate_table_ddl(target_table_schema, table_name, df)
    with target_table_engine.connect(**target_con_params) as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
            error_table_created = True
            print("table for dirty records created")
        except Exception as e:
            print(e)
    
        
    if error_table_created:
        if target_table_type.lower() == 'snowflake':
            with target_table_engine.connect(**target_con_params) as conn:
                cur = conn.cursor()
                df.columns = df.columns.str.upper()
                file_path = f'{table_name}.csv'
                column_list = df.columns
                df.to_csv(file_path, index=False, sep=f'{delimiter}', quotechar=f'{quote_char}', lineterminator='\n')
                sql = f"USE SCHEMA {target_table_schema}"
                try:
                    cur.execute(sql)
                except Exception as e:
                    print(e)
                    cur.close()
                try:
                    file_path = f'{table_name}.csv'
                    cur.execute("PUT file://" + file_path + " @DM")
                except Exception as e:
                    print(e)
                    cur.close()
                sql = f"""COPY INTO {table_name}
                        FROM @DM/{table_name}.csv
                        FILE_FORMAT = (FORMAT_NAME = csv_format FIELD_DELIMITER = '{delimiter}' FIELD_OPTIONALLY_ENCLOSED_BY = '{quote_char}'
                        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"""
                
                try:
                    cur.execute(sql)
                    conn.commit()
                    cur.close()
                    os.remove(file_path)
                    df = None
                except Exception as e:
                    print(e)
                    cur.close()
                sql = f"remove @dm/{table_name}.csv"
                try:
                    cur.execute(sql)
                except Exception as e:
                    print(e)
                    

        elif target_table_type.lower() == 'redshift':
            with target_table_engine.connect(**target_con_params) as conn:
                cur = conn.cursor()
                column_list = df.columns
                column_str = ", ".join(column_list)
                s3 = boto3.client('s3')
                bucket_name = f'{env_prefix}-datalake-s3-land'
                file_name = f'{table_name}.csv'
                file_key = f'redshift_stage/{file_name}'
                df.to_csv(file_name, index=False, sep=f'{delimiter}', quotechar=f'{quote_char}')
                try:
                    s3.upload_file(file_name, bucket_name, file_key)
                except Exception as e:
                    print("Error uploading CSV file to S3:", e)
                sql = f"""
                    COPY {target_table_schema}.{table_name} ({column_str})
                    FROM 's3://{bucket_name}/{file_key}'
                    IAM_ROLE '{redshift_role}'
                    CSV
                    DELIMITER '{delimiter}'
                    ACCEPTINVCHARS '^'
                    QUOTE '{quote_char}'
                    NULL AS '' 
                    IGNOREHEADER 1;
                    """
                try:
                    cur.execute(sql)
                    conn.commit()
                    cur.close()
                    os.remove(file_name)
                    df = None
                    # # Delete file from S3 after successful copy to Redshift
                    # s3.delete_object(Bucket=bucket_name, Key=file_key)
                    # print("File deleted from S3.")
                except Exception as e:
                    print(e)
                
                
        elif target_table_type.lower() in ['rds', 'postgres']:
            # Use rds connector        
            with target_table_engine.connect(**target_con_params) as conn:
                cur = conn.cursor()
                column_list = df.columns
                column_str = ', '.join(column_list)
                sio = io.StringIO()
                df.to_csv(sio, index=None, sep=f'{delimiter}', quotechar=f'{quote_char}')
                sio.seek(0)
                copy_sql = f"COPY {target_table_schema}.{table_name} ({column_str}) FROM STDIN DELIMITER '{delimiter}' CSV HEADER QUOTE '{quote_char}';"
                try:  
                    cur.copy_expert(copy_sql, file=sio)
                    conn.commit()
                    cur.close()
                    sio.close()
                    df = None
                except Exception as e:
                    print(e)
                    cur.close()
                    
    else:
        print("Dirty df not saved")
    

# Function for sending slack messages and alerts 
def send_slack_notification(webhook_url , pretext, message, status:str):
    if status.lower() == "failed":
        color = "#FF0000"
        slack_message = {
                "channel": "#data-management-alerts",
                "attachments": [
                    {
                        "color": color,
                        "pretext": pretext,
                        "text": message
                    }
                ]
            }
        
    elif status.lower() == "success":
        color = "#00FF00"
        slack_message = {
                "channel": "#data-management-airflow-heartbeat",
                "attachments": [
                    {
                        "color": color,
                        "pretext": pretext,
                        "text": message
                    }
                ]
            }
    else:
        color = "#D3D3D3"
        slack_message = {
                "channel": "#data-management-alerts",
                "attachments": [
                    {
                        "color": color,
                        "pretext": pretext,
                        "text": message
                    }
                ]
            }

    # slack_message = {
    #     "channel": "#data_management_notification_test_channel",
    #     "attachments": [
    #         {
    #             "color": color,
    #             "pretext": pretext,
    #             "text": message
    #         }
    #     ]
    # }
    http = urllib3.PoolManager()
    response = http.request(
        'POST',
        webhook_url ,
        body=json.dumps(slack_message),
        headers={'Content-Type': 'application/json'},
        retries=False
    )


def lambda_handler():

    # ############################# parse event object for preliminary info ######################

    req_id = str(datetime.now())
    req_id=datetime.strptime(req_id, '%Y-%m-%d %H:%M:%S.%f')
    req_id=str(req_id)
    print(req_id)
    req_id=str(req_id.replace("-",""))
    req_id=str(req_id.replace(":",""))
    req_id=str(req_id.replace(".",""))
    req_id=str(req_id.replace(" ",""))
    print("req_id", req_id);
    r_configid = os.environ['config_id']
    print("config_id:", r_configid)
    env_prefix = os.environ['stage_prefix']
    region = os.environ['aws_region']
    
        
        
    ################################# setup auxiliary resources ##############################
    try:
        
        session = boto3.Session()
        ssmc = session.client('ssm')
        ssmc = boto3.client('ssm')
        config_catalog_conn_params = ssmc.get_parameter(Name='/{0}/dw_config_catalog_conn_params'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
        config_catalog_conn_params = config_catalog_conn_params.replace("'", "\"")
        config_catalog_conn_params = json.loads(config_catalog_conn_params)
        dw_catalog_conn_params = ssmc.get_parameter(Name='/{0}/dw_catalog_conn_params'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
        dw_catalog_conn_params = dw_catalog_conn_params.replace("'", "\"")
        dw_catalog_conn_params = json.loads(dw_catalog_conn_params)
        
        # get Redshift password and username from parameter store
        rshft_sls_password = ssmc.get_parameter(Name='/{0}/dw-rshft-sls-password'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
        rshft_sls_username = ssmc.get_parameter(Name='/{0}/dw-rshft-sls-username'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
 
        # Update value in connection dictionaries 
        config_catalog_conn_params["user"] = rshft_sls_username
        config_catalog_conn_params["password"] = rshft_sls_password
        config_catalog_conn_params["sslmode"] = "require"
        dw_catalog_conn_params["user"] = rshft_sls_username
        dw_catalog_conn_params["password"] = rshft_sls_password
        dw_catalog_conn_params["sslmode"] = "require"
        rds_conn_params = { 
                    'database': "postgres",
                    'user': "postgres",
                    'port': "5432"
                    }
        #rds_db_ps = ssmc.get_parameter(Name='/{0}/rds-pg-dw-password'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
        #rds_db_endpoint= ssmc.get_parameter(Name='/{0}/rds-pg-dw-endpoint'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
        #rds_db_endpoint_org= rds_db_endpoint.replace(":5432","")
        #rds_conn_params['password'] = rds_db_ps
        #rds_conn_params['host'] = rds_db_endpoint_org
        slack_success_url = ssmc.get_parameter(Name='/{0}/dm-slack-alert-success-url'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
        # snowflake_conn_params = ssmc.get_parameter(Name='/{0}/snowflake_conn_params'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
        # snowflake_conn_params = snowflake_conn_params.replace("'", "\"")
        # snowflake_conn_params = json.loads(snowflake_conn_params)
        slack_failure_url = ssmc.get_parameter(Name='/{0}/dm-slack-alert-failure-url'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
        redshift_role = ssmc.get_parameter(Name='/{0}/redshift_role'.format(env_prefix), WithDecryption=True)['Parameter']['Value']

    except Exception as e:
        print(e)
        raise Exception(e)

    # Etl Query for fetching etl configs  
    etl_query = f"""
            SELECT 
                destinationtype, delimiter, raw_table_mapping, target_table_mapping,
                raw_table_db_type, raw_table_db_name, raw_table_schema, raw_table_name, 
                target_table_db_type, target_table_db_name, target_table_schema, target_table_name, 
                quote_char, data_quality_check, dateformat, has_header, chunk_size, load_type, copy_cmd_type, configjson,file_format
            FROM 
                {etl_config_table} 
            WHERE 
                config_id = {r_configid}
            """ 

    # Initialize the Connection with database for fetching etl config
    print("Establishing Connection with database to get etl configs...")
    try:
        config_db_engine = ConnectionMaker(conn_type='redshift').get_engine()
    except Exception as e:
        status = "failed"
        message = """Could not get etl ETL DB engine from ConnectionMaker.\n\rCheck conn_type parameter.\n\r
        Valid conn_type values for database connection are:\n\r snowflake, rds, postgres, redshift"""
        pretext = "Error getting ETL DB engine from ConnectionMaker"
        send_slack_notification(slack_failure_url , pretext, message, status)
        error_message = f"{pretext}:\n\r{message}\n\r{str(e)}"
        raise Exception(error_message)

    try:
        with config_db_engine.connect(**config_catalog_conn_params) as conn:
            if conn:
                print("Connection with etl config database Established")
            else:
                print("Error connecting to etl db")
                status = 'Failed'
                pretext = "Error Connecting to ETL DB"
                message = "Could Not connect with etl db. Check etl db connection params"
                send_slack_notification(slack_failure_url , pretext, message, status)
                raise Exception("Error connecting to etl db")
            try:
                cur = conn.cursor()
                cur.execute(etl_query)
                etl_config = cur.fetchall()
                cur.close()
            except Exception as e:
                status = "failed"
                pretext = "Error Querying ETL Table"
                message = str(e)
                send_slack_notification(slack_failure_url , pretext, message, status)
                raise Exception(e)
            # Query for getting source file details from etl_config_log table
            file_query = f''' select a.log_id, a.source_path || a.source_file_name  from {etl_log_table} a join {etl_config_table} b on a.config_id=b.config_id  where a.file_status='Extracted' and '.' || reverse(split_part(reverse(a.source_file_name), '.', 1)) = b.filetype and a.config_id = {r_configid} order by a.log_id asc ''' # extract files in definite order ascending


            try:
                cur = conn.cursor()
                cur.execute(file_query)
                file_details = cur.fetchall()
                cur.close()
            except Exception as e:
                print(e)
                status = "failed"
                pretext = "error fetching File details"
                message = f"query: {file_query} could not get file details due to below error:\n\r{e}"
                send_slack_notification(slack_failure_url , pretext, message, status)
                raise Exception(e)
    # Send Alert when querying etl config table fails
    except Exception as e:
        status = "failed"
        pretext = "Error Querying Table"
        message = e
        send_slack_notification(slack_failure_url , pretext, message, status)
        raise Exception(e)
    # Send Alert when etl config is not fetched
    if etl_config is None:
        status = "failed"
        message = f"ETL config either nor defined or Not retrieved for config_id: {r_configid}"
        pretext = "No ETL config found"
        send_slack_notification(slack_failure_url , pretext, message, status)
        error_message = f"{pretext}:\n\r{message}"
        raise Exception(error_message)
        
    # Get configuration values 
    if etl_config:
        for column_values in etl_config:
            source_type = column_values[0]
            delimiter = column_values[1]
            raw_table_mapping = json.loads(column_values[2])
            target_mappings = json.loads(column_values[3])
            target_table_mapping = target_mappings[0]
            primary_key_cols = target_mappings[1]
            raw_tbl_db_type = column_values[4]
            raw_tbl_database = column_values[5]
            raw_tbl_schema = column_values[6]
            raw_tbl_name = column_values[7]
            target_tbl_db_type = column_values[8]
            target_tbl_database = column_values[9]
            target_tbl_schema = column_values[10]
            target_tbl_name = column_values[11]
            quote_char = column_values[12]
            expectation_dict = json.loads(column_values[13])
            date_format = column_values[14]
            has_header = column_values[15]
            chunk_size = column_values[16]
            load_type = column_values[17]
            copy_cmd_type = column_values[18]
            if column_values[19] is not None:
                configjson = json.loads(column_values[19])
            else:
                configjson = ""
            file_format = column_values[20]
            print("Has Header", has_header)
        
    else:
        raise Exception("etl config not defined or extracted.")

    # Set flag for raw table creation
    raw_table_created = False
    if file_details:
        for file_detail in file_details:
            key = file_detail[1]
            print("key:\n",key)
            etl_id = file_detail[0]
            b_name = key.split("/")[0] 
            print("b_name: " + b_name)
            key = key.partition("/")[2]
            if key.endswith("/"):
                print("Not a Valid File Name", key)
                continue 
            filepath = key.split("/")[-2]
            filename = key.split("/")[-1]

            date_part = filename.split('_')[-1].split('.')[0]
            date_part = datetime.strptime(date_part, date_format)
            data_date = date_part.strftime('%Y-%m-%d')
            s3 = boto3.client('s3')
            
            if file_format.lower() == 'fixed width':
                #convert to regular delimited file and proceed
                names = []
                specs = []
                for col in configjson["column_map"]:
                    names.append(col["col_name"])
                    specs.append(ast.literal_eval(col["col_spec"]))
                skiprows = configjson["skip_rows"]
                response = s3.get_object(Bucket=b_name,Key=key)
                file = response["Body"].read()
                #df_fwf = wr.s3.read_fwf(f's3://{b_name}/{key}', header=None, skipfooter=1, skiprows=skiprows, names=names,colspecs=specs, dtype=str, encoding_errors = 'ignore')
                df_fwf = pd.read_fwf(io.BytesIO(file),header=0, skiprows=skiprows, names=names,colspecs=specs, dtype=str, encoding_errors = 'ignore')
                df_fwf = df_fwf.fillna('').astype(str)
                csv_buffer = StringIO()
                df_fwf.to_csv(csv_buffer, sep='{0}'.format(delimiter), index=False)
                key = 'fwf/' + key
                s3.put_object(Bucket=b_name, Body=csv_buffer.getvalue(), Key=key)

            
                       
            # # Set import Process start time
            import_start = datetime.now()
            log_etl_load_info(config_db_engine, r_configid, config_catalog_conn_params, etl_id, import_startts=import_start)

            # Check Blank File
            sample_size = 50*1024 #50kb
            try:
                sample_data = s3.get_object(Bucket=b_name, Key=key)['Body'].read(sample_size).decode('utf-8', errors='ignore')
                num_rows = sample_data.count('\n')
                print("Number of rows in sample data:", num_rows)
            except Exception as e:
                print(e)
                log_etl_load_info(config_db_engine, r_configid, config_catalog_conn_params, etl_id, file_status='Extracted/Failed', insertion_rowcount=0, datadate=data_date, runid=req_id, raw_table_db_type=raw_tbl_db_type, raw_table_db_name = raw_tbl_database, raw_table_name = raw_tbl_name, raw_table_schema = raw_tbl_schema, raw_table_load_status = "FAILED", raw_table_load_description = str(e)) # Mark File as Failed in log table
                status = "FAILED" # send slack notification
                pretext = "Error Reading File From s3"
                message = f"""Skipping Raw Table and Target Table Load for file {filepath}/{filename}\n
                \rCould not read file from s3 due to below error:\n
                \r{str(e)}"""
                send_slack_notification(slack_success_url , pretext, message, status)
                raise Exception(e)

            
            if has_header and num_rows == 1 or num_rows == 0:
                print(f"The File: {filepath}/{filename} is blank")
                print("Skipping File Load")
                #log_etl_load_info(config_db_engine, r_configid, config_catalog_conn_params, etl_id, file_status='Extracted/Skipped/Blank', insertion_rowcount=0, datadate=data_date, runid=req_id)
                
                # Log to etl config Log Table
                log_etl_load_info(config_db_engine, r_configid, config_catalog_conn_params, etl_id, file_status="Extracted/Skipped/Blank", insertion_rowcount=0, datadate=data_date, runid=req_id, raw_table_db_type=raw_tbl_db_type, raw_table_db_name = raw_tbl_database, raw_table_name = raw_tbl_name, raw_table_schema = raw_tbl_schema, raw_table_load_status = "SUCCESS", raw_table_load_description = "Table Loaded Successfully", delimiter = delimiter, quotechar=quote_char, has_header=has_header, target_table_db_type=target_tbl_db_type, target_table_db_name=target_tbl_database, target_table_name=target_tbl_name, target_table_schema=target_tbl_schema, target_table_load_status="SUCCESS",
                target_table_load_description="Table Loaded Successfully")

                status = "SUCCESS" # send slack notification
                pretext = "Blank File "
                message = f"Skipping Raw Table and Target Table Load for file {filepath}/{filename} is blank"
                send_slack_notification(slack_success_url , pretext, message, status)

                continue

            # Create a temporary csv file from the sample data
            temp_dir = tempfile.mkdtemp()
            temp_file_path = os.path.join(temp_dir, 'temporary_file.csv')
            with open(temp_file_path, 'w') as f:  
                f.write(sample_data)

            df_temp = pd.read_csv(temp_file_path, delimiter=delimiter, quotechar=quote_char, nrows=2, header=None if not has_header else 0, encoding_errors='ignore')

            if has_header: 
                missing_columns, extra_columns, missing_required_columns = pre_process_column_mapping(raw_table_mapping, df_temp)

                if missing_required_columns:
                    print("Skipping Table load")
                    print(f"File: {filepath}/{filename} is missing required columns")
                    print("missing required column names:", missing_required_columns)
                    # Send Slack Alert
                    status = "Failed"
                    pretext = f"File {filepath}/{filename} is missing required columns"
                    message = f"""File {filepath}/{filename} is missing required columns:\n\r{missing_required_columns}\n\r
                    Skipping Target Table and Raw Table Load"""
                    send_slack_notification(slack_failure_url , pretext, message, status)
                    # Log to etl config Log Table
                    log_etl_load_info(config_db_engine, r_configid, config_catalog_conn_params, etl_id, file_status="Extracted/Skipped",raw_table_db_type=raw_tbl_db_type, raw_table_db_name = raw_tbl_database, raw_table_name = raw_tbl_name, raw_table_schema = raw_tbl_schema, raw_table_load_status = "SKIPPED", raw_table_load_description = f"Table Load Skipped. File Missing required columns, {str(missing_required_columns)}", delimiter = delimiter, quotechar=quote_char, has_header=has_header, missing_columns=str(missing_columns), extra_columns=str(extra_columns), target_table_db_type=target_tbl_db_type, target_table_db_name=target_tbl_database, target_table_name=target_tbl_name, target_table_schema=target_tbl_schema, target_table_load_status="SKIPPED",
                    target_table_load_description=f"Table Load Skipped. File Missing required columns, {str(missing_required_columns)}")
                    continue

            
            try:
                if raw_tbl_db_type.lower() in ['postgres', 'rds']:
                    raw_db_engine = ConnectionMaker(conn_type='rds').get_engine()
                    raw_db_params = rds_conn_params
                elif raw_tbl_db_type.lower() == 'redshift':
                    raw_db_engine = ConnectionMaker(conn_type='redshift').get_engine()
                    raw_db_params = config_catalog_conn_params
                # elif raw_tbl_db_type.lower() == 'snowflake':
                #     raw_db_engine = ConnectionMaker(conn_type='snowflake').get_engine()
                #     raw_db_params = snowflake_conn_params
                else:
                    print(f"Not a Valid db type: {raw_tbl_db_type}")
                    raise ValueError(f"Not a valid db type: {raw_tbl_db_type}")
            except Exception as e:
                print(e)
                raise Exception(e)

            
            try:
                print("raw table load start at:", datetime.now())
                raw_table_creation_status = True #load_raw_table(raw_tbl_db_type, raw_db_engine, raw_tbl_schema, raw_tbl_name, b_name, key, delimiter, quote_char, df_temp, region, redshift_role, raw_table_mapping, raw_table_created, has_header, copy_cmd_type, **raw_db_params)
                raw_table_created = raw_table_creation_status
                print("raw table load complete at:", datetime.now())


                if not has_header or (not missing_columns and not extra_columns):
                    missing_columns_str = None
                    extra_columns_str = None
                elif not missing_columns:
                    missing_columns_str=None
                    extra_columns_str = ', '.join(extra_columns)
                elif not extra_columns:
                    extra_columns_str = None
                    missing_columns_str = ', '.join(missing_columns)
                else:
                    missing_columns_str = ', '.join(missing_columns)
                    extra_columns_str = ', '.join(extra_columns)


                now = datetime.now()
                log_etl_load_info(config_db_engine, r_configid, config_catalog_conn_params, etl_id, raw_table_db_type=raw_tbl_db_type, raw_table_db_name = raw_tbl_database, raw_table_name = raw_tbl_name, raw_table_schema = raw_tbl_schema, raw_table_load_status = "SUCCESS", raw_table_load_description = "Table Loaded Successfully", raw_table_load_date = now, delimiter = delimiter, quotechar=quote_char, has_header=has_header, 
                missing_columns=missing_columns_str, extra_columns=extra_columns_str)

                status = "SUCCESS" # send slack notification
                pretext = f"{raw_tbl_schema}.{raw_tbl_name} Load SUCCEEDED"
                message = f"""{raw_tbl_schema}.{raw_tbl_name} has completed successfully for file
                {filepath}/{filename}"""
                send_slack_notification(slack_success_url , pretext, message, status)
            except Exception as e:
                print(e)
                log_etl_load_info(config_db_engine, r_configid, config_catalog_conn_params, etl_id, file_status="Extracted/Failed", raw_table_db_type=raw_tbl_db_type, raw_table_db_name = raw_tbl_database, raw_table_name = raw_tbl_name, raw_table_schema = raw_tbl_schema, raw_table_load_status = "FAILED", raw_table_load_description = str(e), delimiter = delimiter, quotechar=quote_char, has_header=has_header, missing_columns=str(missing_columns), extra_columns=str(extra_columns)) # Mark file as Failed when raw table load fails
                
                status = "FAILED" # send slack notification
                pretext = f"{raw_tbl_schema}.{raw_tbl_name} Load Failed"
                message = f"""{raw_tbl_schema}.{raw_tbl_name} has load has failed for file
                {filepath}/{filename} due to below error:\n\r{str(e)}"""
                send_slack_notification(slack_failure_url , pretext, message, status)
                raise Exception(e)

            # remove temporary directory
            if temp_dir:
                shutil.rmtree(temp_dir)


            if target_tbl_db_type.lower() in ['rds', 'postgres']:
                target_db_engine = ConnectionMaker(conn_type='rds').get_engine()
                target_db_params = rds_conn_params
            elif target_tbl_db_type.lower() == 'redshift':
                target_db_engine = ConnectionMaker(conn_type='redshift').get_engine()
                target_db_params = dw_catalog_conn_params
            # elif target_tbl_db_type.lower() == 'snowflake':
            #     target_db_engine = ConnectionMaker(conn_type='snowflake').get_engine()
            #     target_db_params = snowflake_conn_params
            else:
                print("Not a Valid Target Table db type")
                raise ValueError(f"Not a Valid Target Table db type: {target_tbl_db_type}")
                                    
            target_start = datetime.now()
            print("Target Load Time Start", target_start)
            
            try:  
                now = datetime.now()
                dirty_df_list = load_target_table (target_tbl_db_type, target_db_engine, target_table_mapping, target_tbl_schema, target_tbl_name, expectation_dict, data_date, etl_id, now, req_id, primary_key_cols, target_db_params, delimiter, quote_char, redshift_role, env_prefix, has_header, b_name, key, missing_columns, chunk_size, load_type)
                    
                target_load_end_time = datetime.now()
                print("target load end time", target_load_end_time)
            
            except Exception as e:
                print(e)
                log_etl_load_info(config_db_engine, r_configid, config_catalog_conn_params, etl_id, target_table_db_type=target_tbl_db_type, target_table_db_name = target_tbl_database, target_table_name = target_tbl_name, target_table_schema = target_tbl_schema, target_table_load_status = "FAILED", target_table_load_description = str(e), file_status='Extracted/Failed',
                datadate=data_date, runid=req_id)
                
                status = "FAILED" # Send slack notification
                pretext = f"{target_tbl_schema}.{target_tbl_name} Load Failed"
                message = f"""{target_tbl_schema}.{target_tbl_name}  Load Failed due to below error for file
                {filepath}/{filename}due to below error:\n\r{str(e)}"""
                send_slack_notification(slack_failure_url , pretext, message, status)

                raise Exception(e)
            
            target_table_load_description = "Table Loaded Successfully"
            try:
                if dirty_df_list:

                    dirty_records_df = pd.concat(dirty_df_list) 
                    save_error_records_to_table(dirty_records_df, target_db_engine, target_tbl_db_type, error_table_schema, target_tbl_name, data_date, config_catalog_conn_params, redshift_role, delimiter, quote_char, etl_id, env_prefix)

                    dirty_records_count = dirty_records_df.shape[0] 
                    target_table_load_description = ', '.join(dirty_records_df['checks_failed'].unique())

                    status = "FAILED" # Send slack Alert
                    pretext = f"Dirty data detected in file {filepath}/{filename}"
                    message = f"""Dirty data has been detected in file: {filepath}/{filename} for target table:
                    {target_tbl_schema}.{target_tbl_name}\n
                    \rBad Rows Removed: {dirty_records_count}"""
                    send_slack_notification(slack_failure_url , pretext, message, status)
                else:
                    dirty_records_count = 0
            except Exception as e:
                print(e)

            try:
                sql = f"SELECT count(*) FROM {target_tbl_schema}.{target_tbl_name} WHERE etl_id = {etl_id}"
                with target_db_engine.connect(**target_db_params) as conn:
                    cur = conn.cursor()
                    cur.execute(sql)
                    records = cur.fetchall()[0][0]
                    cur.close()

                print("records inserted", records)
                # Update Log Table 
                log_etl_load_info(config_db_engine, r_configid, config_catalog_conn_params, etl_id, target_table_db_type=target_tbl_db_type, target_table_db_name = target_tbl_database, target_table_name = target_tbl_name, target_table_schema = target_tbl_schema, target_table_load_status = "SUCCESS", target_table_load_description = target_table_load_description, target_table_load_date = target_load_end_time, import_completets = target_load_end_time, insertion_rowcount=records, file_status='Extracted/Loaded', failed_rows_count = dirty_records_count, datadate=data_date, runid=req_id)
                
                status = "SUCCESS" # send slack Notification
                pretext = f"{target_tbl_schema}.{target_tbl_name} Load SUCCEEDED"
                message = f"""{target_tbl_schema}.{target_tbl_name} has completed successfully for file
                {filepath}/{filename}\n
                \rInsertion Row Count: {records}\n
                \rFailed Row Count: {dirty_records_count}"""
                send_slack_notification(slack_success_url , pretext, message, status)
            except Exception as e:
                print(e)

            try:
                db_npiencryptkey = ssmc.get_parameter(Name='/{0}/rds-npi-encryptkey'.format(env_prefix), WithDecryption=True)['Parameter']['Value']
                sql = f'''SELECT postetl_query FROM app.postetl_process WHERE lake_tbl_name = '{target_tbl_name}' '''
                with config_db_engine.connect(**config_catalog_conn_params) as conn:
                    cur = conn.cursor()
                    cur.execute(sql)
                    res = cur.fetchall()
                with target_db_engine.connect(**target_db_params) as conn:
                    cur = conn.cursor()
                    for r in res:
                        sql = str(r[0]).replace('@EncryptKey','{0}').format(db_npiencryptkey)
                        print(sql)
                        sql_split = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
                        for stmt in sql_split:
                            cur.execute(stmt)                        
                            conn.commit()
                            print(f'Post etl query executed sucessfully for : {target_tbl_name}')
                status = 'success'
                pretext = 'Success Executing post etl query'
                message = f'post etl query executed successfully for table: {target_tbl_name}'
                print(message)
                send_slack_notification(slack_success_url , pretext, message, status)
            except Exception as e:
                status = 'failed'
                pretext = 'Error Executing post etl query'
                message = f'Error while trying to execute post etl query for table: {target_tbl_name} due to error\n\r{e}'
                print(message)
                send_slack_notification(slack_failure_url , pretext, message, status)
                raise Exception(e)



    else:
        print("File details not retrieved")
        print("terminating data load")
        # raise Exception("File details Not received") # Stopped sending error when no file to process
            
                
if __name__ == "__main__":
    lambda_handler()
