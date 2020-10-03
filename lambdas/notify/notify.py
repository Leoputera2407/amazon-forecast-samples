import os
import boto3
import json
from io import StringIO 
import snowflake.connector
import math
import pandas as pd
from loader import Loader

LOADER = Loader()

def get_datasetID(datasets):
    data = datasets[0]
    if data['DatasetType'] == "TARGET_TIME_SERIES":
        separator = "_TARGET"
    elif data['DatasetType'] == "RELATED_TIME_SERIES":
        separator = "_RELATED"
    else:
        separator = "_META"
    return data['DatasetName'].split(separator)[0]
    


def snowflake_connect():
    # Check and make sure the credentials were pulled correctly
    try:
        # Connect to snowflake
        """
        ctx = snowflake.connector.connect(
            #Credentials here
        )
        return ctx.cursor()
        """
    except snowflake.connector.errors.ProgrammingError as e:
        LOADER.logger.info(
                    'Failed to connect to snowflake: {0): {1}}'.format(e.errno, e.msg) 
                )

def lambda_handler(event, context):
    forecast_bucket_arn = os.environ['BUCKET_ARN']
    bucket_name = forecast_bucket_arn.split(":::")[1]
    forecasts = event['params']['Forecast']
    datasetID =  get_datasetID(event['params']['Datasets'])
    PREDICTOR = {
        "arn:aws:forecast:::algorithm/CNN-QR" : "CNNQR",
        "arn:aws:forecast:::algorithm/Deep_AR_Plus": "DEEPARP",
        "arn:aws:forecast:::algorithm/Prophet": "PROPHET",
        "arn:aws:forecast:::algorithm/NPTS":  "NPTS",
        "arn:aws:forecast:::algorithm/ARIMA": "ARIMA",
        "arn:aws:forecast:::algorithm/ETS": "ETS"
    }

    for idx, forecast in enumerate(forecasts):
        predictor = event['params']['Predictor'][idx]
        predictorName = predictor['PredictorName']
        assert(predictorName == forecast['PredictorName'])
        model_s3path = forecast['s3path']
        metric = forecast['metric']
        if predictor['PerformAutoML'] == True:
            Model = 'AUTOML'
        else:
            Model = PREDICTOR[predictor['AlgorithmArn']]
        #Get forecast values from S3 into pandas    
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket_name)
        prefix_objs = bucket.objects.filter(Prefix=predictorName)
        print(prefix_objs)
        prefix_df = []
        for obj in prefix_objs:
            if "csv" in obj.key:
                body = obj.get()['Body']
                csv_string = body.read().decode('utf-8')
                df = pd.read_csv(StringIO(csv_string))
                prefix_df.append(df)
    
        prefix_df= pd.concat(prefix_df)
        prefix_df['date'] = prefix_df['date'].apply(lambda x: x.split('T')[0])
        # Convert Quantile back to un-logged value
        for quantile in list(prefix_df.columns[2:]):
            prefix_df[quantile] = prefix_df[quantile].apply(lambda x: math.exp(x) -1)
        #Insert [Region,Country_group]
        prefix_df['Region'] = prefix_df.apply(lambda row : insert_region_country(row), axis =1 )
    
        #Code if you want to push to snowflake
        """
        #Snowflake Setup
        cur = snowflake_connect()
        
        #Push forecast into Snowflake
        for item_id in list(prefix_df.item_id.unique()):
            region_df = prefix_df.loc[prefix_df['item_id'] == item_id]
            date = list(region_df['date'])
            forecast_list = []
            for quantile in list(region_df.columns[2:-1]):
                forecast ={}
                forecast['name'] = quantile
                forecast['value'] = list(region_df[quantile])
                forecast_list.append(forecast)
            result ={}
            result['date'] = date
            result['forecast'] = forecast_list
   
            region = region_df['Region'].iloc[0][0]
            country_group = region_df['Region'].iloc[0][1]
            franchise = item_id.split('-')[0].upper()
            try:
                cur.execute('INSERT INTO ' + os.environ['SNOWFLAKE_FORECAST_TABLE'] +' (dataset_id, model_name, franchise, region, country_group, forecast)\
                    SELECT '
                    + "' "+ datasetID + " ',"
                    + "' "+ Model + " ',"
                    + "' "+ franchise +" ',"
                    + "' "+ region + " ',"
                    + "' "+ country_group + " ',"
                    + "parse_json(' "
                    + json.dumps(result) + " ');"
                )
            except snowflake.connector.errors.ProgrammingError as e:
                LOADER.logger.info(
                    'Failed to insert to forecast table in snowflake: {0): {1}}'.format(e.errno, e.msg) 
                )
               
        #INSERT metric to forecast_metric
        try:
            cur.execute('INSERT INTO ' + os.environ['SNOWFLAKE_FORECAST_METRIC_TABLE'] + ' (dataset_id, MODEL_NAME, model_s3path, metric)\
            SELECT '
            + "' " + datasetID + " ',"
            + "' " + Model + " ',"
            + "' " + model_s3path + " ',"
            + "parse_json(' "
            + json.dumps(metric) + " ');"
            )
        except snowflake.connector.errors.ProgrammingError as e:
            LOADER.logger.info(
                'Failed to insert to forecast metric table in snowflake: {0): {1}}'.format(e.errno, e.msg) 
            )
    #Once all forecasts are added, then update that they're available   
    try:
        cur.execute("UPDATE " + os.environ['SNOWFLAKE_PARAM_TABLE'] + " SET forecast_available = 'true' where DATASET_ID= '" + datasetID + "';")
    except snowflake.connector.errors.ProgrammingError as e:
            LOADER.logger.info(
                'Failed to insert to update param_table in snowflake: {0): {1}}'.format(e.errno, e.msg) 
            )
    LOADER.logger.info(
                'Successfully pushed to all Snowflake table' 
            )
    """
    return (event)