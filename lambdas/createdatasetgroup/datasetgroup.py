from os import environ
import actions
from loader import Loader

ARN = 'arn:aws:forecast:{region}:{account}:dataset-group/{name}'
LOADER = Loader()

def lambda_handler(event, context):
    dataset_group = event['params']['DatasetGroup']
    status = None
    event['params']['DatasetGroup']['DatasetGroupArn'] = ARN.format(
        account=event['params']['misc']['AccountID'],
        name=dataset_group['DatasetGroupName'],
        region=environ['AWS_REGION']
    )
    try:
        status = LOADER.forecast_cli.describe_dataset_group(
            DatasetGroupArn=event['params']['DatasetGroup']['DatasetGroupArn']
        )
        
    except LOADER.forecast_cli.exceptions.ResourceNotFoundException:
        LOADER.logger.info(
            'Dataset Group not found! Will follow to create Dataset Group.'
        )
        dataset_arns =[]
        datasets = event['params']['Datasets']
        for dataset in datasets:
            dataset_arns.append(dataset['DatasetArn'])
        LOADER.forecast_cli.create_dataset_group(
            DatasetGroupName= dataset_group['DatasetGroupName'],
            Domain = dataset_group['Domain'],
            DatasetArns=dataset_arns    
        )
        status = LOADER.forecast_cli.describe_dataset_group(
            DatasetGroupArn=event['params']['DatasetGroup']['DatasetGroupArn']
        )
    actions.take_action(status['Status'])
    return event
