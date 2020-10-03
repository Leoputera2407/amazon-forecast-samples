from os import environ
from boto3 import client
import actions
from loader import Loader

ACCOUNTID = client('sts').get_caller_identity()['Account']
ARN = 'arn:aws:forecast:{region}:{account}:dataset/{name}'
LOADER = Loader()


def lambda_handler(event, context):
 datasets = event['params']['Datasets']
 status = None
 arns={}
 for dataset in datasets:
  dataset_name = dataset['DatasetName']
  arns[dataset_name] = ARN.format(
   account=ACCOUNTID,
   name=dataset_name,
   region=environ['AWS_REGION']
   )
 event['params']['misc']['AccountID'] = ACCOUNTID
 #If Dataset Already Exists
 try:
  status =[]
  for arn in arns.values():
   stat = LOADER.forecast_cli.describe_dataset(
    DatasetArn=arn
    )
  status.append(stat['Status'])
 except LOADER.forecast_cli.exceptions.ResourceNotFoundException:
  LOADER.logger.info('Dataset not found! Will follow to create dataset.')
  status = []
  for idx, dataset in enumerate(datasets):
   LOADER.forecast_cli.create_dataset(
    Domain= dataset['Domain'],
    DatasetType=dataset['DatasetType'],
    DatasetName=dataset['DatasetName'],
    DataFrequency=dataset['DataFrequency'],
    Schema = dataset['Schema']
   )
   stat = LOADER.forecast_cli.describe_dataset(
    DatasetArn= arns[dataset['DatasetName']]
   )
   status.append(stat['Status'])
 for idx,dataset in enumerate(datasets):
  event['params']['Datasets'][idx]['DatasetArn'] = arns[dataset['DatasetName']]
 actions.take_action(status)
 return event