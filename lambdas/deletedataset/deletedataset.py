import actions
from loader import Loader

LOADER = Loader()


def lambda_handler(event, context):
    datasets = event['params']['Datasets']
    try:
        for dataset in datasets:
            LOADER.forecast_cli.delete_dataset_import_job(
                DatasetImportJobArn=dataset['DatasetImportJobArn']
            )
        for dataset in datasets:
            actions.take_action_delete(
                LOADER.forecast_cli.describe_dataset_import_job(
                    DatasetImportJobArn=dataset['DatasetImportJobArn']
                )['Status']
            )

    except (LOADER.forecast_cli.exceptions.ResourceNotFoundException, KeyError):
        LOADER.logger.info('Import job not found! Passing.')
        
    try:
        for dataset in datasets:
            LOADER.forecast_cli.delete_dataset(
                DatasetArn=dataset['DatasetArn']
            )
        for dataset in datasets:
            actions.take_action_delete(
                LOADER.forecast_cli.describe_dataset(
                    DatasetArn=dataset['DatasetArn']
                )['Status']
            )

    except (LOADER.forecast_cli.exceptions.ResourceNotFoundException, KeyError):
        LOADER.logger.info('Import job not found! Passing.')
    
    datasetgroup = event['params']['DatasetGroup']    
    try:
        LOADER.forecast_cli.delete_dataset_group(
            DatasetGroupArn=datasetgroup['DatasetGroupArn']
        )
        actions.take_action_delete(
            LOADER.forecast_cli.describe_dataset_group(
                DatasetGroupArn=datasetgroup['DatasetGroupArn']
            )['Status']
        )
    except (LOADER.forecast_cli.exceptions.ResourceNotFoundException, KeyError):
        LOADER.logger.info('Import job not found! Passing.')
        
        
    
    return event