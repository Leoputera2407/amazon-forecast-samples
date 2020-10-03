import os
from json import loads, dumps
from datetime import datetime
from boto3 import client
import boto3
import snowflake.connector
import pandas as pd
import math
from io import StringIO, BytesIO
from schema import fill_schema
import csv
from loader import Loader

LOADER = Loader()

STEP_FUNCTIONS_CLI = client('stepfunctions')

def snowflake_connect():
    # Check and make sure the credentials were pulled correctly
    try:
        """
        # Connect to snowflake
        ctx = snowflake.connector.connect(
            #Credentials here   
        )
        return ctx.cursor()
        """
    except snowflake.connector.errors.ProgrammingError as e:
        LOADER.logger.info(
                    'Failed to connect to snowflake: {0): {1}}'.format(e.errno, e.msg) 
                )
        return None
    

def snowflake_to_panda(cur, query):
    cur.execute(query)
    df = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    return df

def panda_unload_to_s3(df, bucket, filename):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,header=False, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())
    
def log_arget_value(df):
    df['TARGET_VALUE'] = df['TARGET_VALUE'].astype('float64').apply(lambda x: math.log(x+1))
    
def format_weekly_dates(df):   
    df['TIMESTAMP'] = df['TIMESTAMP'].apply(lambda x: x.split(' ')[0])

def handle_negative_target(df):
    #Keep only values > 0, NaN everything else
    df = df.assign(TARGET_VALUE = lambda x: x.TARGET_VALUE.where(x.TARGET_VALUE.ge(0)))
    #Use ffil to replace NaN with the first previous values that are non-NaN
    df = df.fillna(method='ffill')
    return df

def lambda_handler(event, context):
    for record in event['Records']:
        payload = loads(record['body'])
        #payload = record['body']
        dataname = payload['Dataname']
        deleteFlag = payload['PerformDelete']
        forecastHorizon = payload['ForecastHorizon']
        forecast_bucket_arn = os.environ['TARGET_BUCKET']
        bucket_name = forecast_bucket_arn.split(":::")[1]
        
        forecast_bucket = "s3://" + bucket_name
        target_name =  dataName + "/target.csv"
        related_name =  dataName + "/related.csv"
        validate_name = dataName + "/validate.csv"
        target_bucket = forecast_bucket + "/" + target_name
        related_bucket = forecast_bucket + "/" + related_name
        validate_bucket = forecast_bucket + "/" + validate_name
        
        # Get dataset from Snowflake
        cur = snowflake_connect()

        if cur != None:
            try:
                target_query = ""
                
                target_df = snowflake_to_panda(cur, target_query)
                LOADER.logger.info(
                    'Done getting target dataset, which has {0} data points'.format(len(target_df))
                )
                target_df = handle_negative_target(target_df)
                log_target_value(target_df)
                panda_unload_to_s3(target_df, bucket_name, target_name)
                
                related_query = ""
                related_df = snowflake_to_panda(cur, related_query)
                panda_unload_to_s3(related_df, bucket_name, related_name)
                LOADER.logger.info(
                    'Done getting related dataset, which has {0} data points'.format(len(related_df))
                )
                
                validate_query = ""
                target_df = handle_negative_target(target_df)
                log_target_value(validate_df)
                panda_unload_to_s3(validate_df, bucket_name, validate_name)
                LOADER.logger.info(
                    'Done getting related dataset, which has {0} data points'.format(len(validate_df))
                )
                
            except snowflake.connector.errors.ProgrammingError as e: 
                LOADER.logger.info(
                    'Failed to query to snowflake! Error {0} ({1}): {2}'.format(e.errno, e.sqlstate, e.msg,)
                )
            finally:
                cur.close()

            
        params =  fill_schema(payload, target_bucket, related_bucket, deleteFlag)
        #return(params)

        return dumps(
        STEP_FUNCTIONS_CLI.start_execution(
            stateMachineArn=os.environ['STEP_FUNCTIONS_ARN'],
            name= dataName +"_" + datetime.now().strftime("%Y_%m_%d"),
            input=dumps(
                {
                    'params': params
                }
            )
        ),
        default=str
        )
        