from datetime import datetime
import os

PARAM_SCHEMA = {
    "Datasets": [],
    "DatasetGroup": {
      "DatasetGroupName": "",
      "Domain": "CUSTOM"
    },
    "misc": {
      "TimestampFormat": "yyyy-MM-dd",
      "currentDate": "",
      "bucket": ""
    },
    "Predictor": [],
    "Forecast": [],
    "PerformDelete": False
}

#TODO: For now, just hard-coded the schema
TARGET_DATASET_SCHEMA = {
        "Domain": "CUSTOM",
        "DatasetType": "TARGET_TIME_SERIES",
        "DatasetName": "",
        "DataFrequency": "",
        "Schema": {
            "Attributes":[
                {
                 "AttributeName":"timestamp",
                 "AttributeType":"timestamp"
                 },
                {
                "AttributeName":"item_id",
                "AttributeType":"string"
                },
                {
                "AttributeName":"target_value",
                "AttributeType":"float"
                }
            ]
        },
        "s3path": ""
}


#TODO: For now, just hard-coded the schema
RELATED_DATASET_SCHEMA =  {
        "Domain": "CUSTOM",
        "DatasetType": "RELATED_TIME_SERIES",
        "DatasetName": "",
        "DataFrequency": "",
        "Schema": {
            "Attributes":[
                {
                    "AttributeName":"Timestamp",
                    "AttributeType":"timestamp"
                }
            ]
        },
        "s3path": ""
      }

PREDICTOR_SCHEMA = {
        "PredictorName": "",
        "ForecastHorizon": 0,
        "PerformAutoML": False,
        "PerformHPO": False ,
        "EvaluationParameters": {
          "NumberOfBacktestWindows": 0,
          "BackTestWindowOffset": 0
        },
        "FeaturizationConfig": {
          "ForecastFrequency": "",
          "Featurizations": [
            {
              "AttributeName": "target_value",
              "FeaturizationPipeline": [
                {
                  "FeaturizationMethodName": "filling",
                  "FeaturizationMethodParameters": {
                    "frontfill": "none",
                    "middlefill": "mean",
                    "backfill": "mean"
                  }
                }
              ]
            }
          ]
        }
      }

#TODO: For now, just hard-coded the ForecastTypes  
FORECAST_SCHEMA =  {
        "ForecastName": "",
        "PredictorName": "",
        "ForecastTypes": [
          "0.50",
          "0.80",
          "0.90",
          "0.95",
          "0.99"
        ]
      }

      
PREDICTOR_ARN = {
        "CNNQR": "arn:aws:forecast:::algorithm/CNN-QR",
        "DEEPARP": "arn:aws:forecast:::algorithm/Deep_AR_Plus",
        "PROPHET": "arn:aws:forecast:::algorithm/Prophet",
        "NPTS": "arn:aws:forecast:::algorithm/NPTS",
        "ARIMA": "arn:aws:forecast:::algorithm/ARIMA",
        "ETS": "arn:aws:forecast:::algorithm/ETS"
}

def fill_data_schema(event, data_schema, s3path, data_type):
  schema = data_schema.copy()
  schema['DatasetName'] = event['DataName'] + "_" + data_type
  schema['DataFrequency'] = event['ForecastFrequency']
  schema['s3path'] = s3path
  return schema
  


def populate_predictor_schema(event):
  pred_list =[]
  predictors = event['Models']
  dataName = event['DataName']
  
  for predictor in predictors:
    pred = PREDICTOR_SCHEMA.copy()
    predictor_name = dataName + "_" + predictor + "_" + str(event['NumberOfBacktestWindows'])
    pred['PredictorName'] = predictor_name
    pred['ForecastHorizon'] = event["ForecastHorizon"]

    if predictor in PREDICTOR_ARN.keys():
      pred['AlgorithmArn'] = PREDICTOR_ARN[predictor]
      pred['PerformAutoML'] = False
    else:
      pred['PerformAutoML'] = True
    
    if predictor in ['CNNQR', 'DEEPARP']:
      pred['PerformHPO'] = True
      pred['PredictorName'] = pred['PredictorName'] + "_hpo"
    else:
      pred['PerformHPO'] = False
      
    pred['EvaluationParameters']['NumberOfBacktestWindows'] = event["NumberOfBacktestWindows"]
    pred['EvaluationParameters']['BackTestWindowOffset'] = event["ForecastHorizon"]
    pred['FeaturizationConfig']['ForecastFrequency'] = event['ForecastFrequency']
    pred_list.append(pred)

  return pred_list
  
def fill_forecast_schema(predictors):
  forecast_list =[]
  for pred in predictors:
    forecast = FORECAST_SCHEMA.copy()
    predictor_name =  pred['PredictorName']
    forecast['ForecastName'] = predictor_name + "_forecast"
    forecast['PredictorName'] = predictor_name
    forecast_list.append(forecast)
  return forecast_list
  

def fill_schema(event, target_s3path, related_s3path, deleteFlag):
  param = PARAM_SCHEMA
  
  datasets = []
  target_data = fill_data_schema(event, TARGET_DATASET_SCHEMA, target_s3path, "TARGET")
  datasets.append(target_data)
  related_data = fill_data_schema(event, RELATED_DATASET_SCHEMA, related_s3path, "RELATED")
  datasets.append(related_data)
  #TODO: Insert Metadata?
  
  param['Datasets'] = datasets

  
  param['DatasetGroup']['DatasetGroupName'] = event['DataName'] + "_group"
  
  param['misc']['currentDate'] = event['dateCreate']
  FORECAST_BUCKET_ARN	 = os.environ['TARGET_BUCKET'] 
  bucket_name =  FORECAST_BUCKET_ARN.split(":::")[1] 
  param['misc']['bucket'] = bucket_name
  
  predictors =  populate_predictor_schema(event)
  param['Predictor'] = predictors
  
  param['Forecast'] = fill_forecast_schema(predictors)
  param['PerformDelete'] = deleteFlag

  return param