from os import environ
import actions
from loader import Loader

ARN = 'arn:aws:forecast:{region}:{account}:dataset-import-job/{name}/{name}_{date}'
LOADER = Loader()


def lambda_handler(event, context):
    params = event['params']
    datasets = event['params']['Datasets']
    status = None
    arns={}
    for dataset in datasets:
        dataset_name = dataset['DatasetName']
        arns[dataset_name] = ARN.format(
            account=params['misc']['AccountID'],
            date=params['misc']['currentDate'],
            name=dataset_name,
            region=environ['AWS_REGION']
        )
    
    try:
        status = []
        for dataset in datasets:
            stat = LOADER.forecast_cli.describe_dataset_import_job(
                DatasetImportJobArn= arns[dataset['DatasetName']]
            )
            print(stat['Status'])
            status.append(stat['Status'])
    except LOADER.forecast_cli.exceptions.ResourceNotFoundException:
        LOADER.logger.info(
            'Dataset import job not found! Will follow to create new job.'
        )
        status = []
        for idx, dataset in enumerate(datasets):
            LOADER.forecast_cli.create_dataset_import_job(
                DatasetImportJobName='{name}_{date}'.format(
                    name=dataset['DatasetName'],
                    date=params['misc']['currentDate']
                    ),
                DatasetArn=dataset['DatasetArn'],
                DataSource={
                    'S3Config':
                        {
                            'Path':
                                dataset['s3path'],
                            'RoleArn':
                                environ['FORECAST_ROLE']
                        }
                    
                },
                TimestampFormat=params['misc']['TimestampFormat']
                )
            stat = LOADER.forecast_cli.describe_dataset_import_job(
                DatasetImportJobArn= arns[dataset['DatasetName']]
                )
            status.append(stat['Status'])
    for idx,dataset in enumerate(datasets):
        event['params']['Datasets'][idx]['DatasetImportJobArn'] = arns[dataset['DatasetName']]
    actions.take_action(status)
    return event