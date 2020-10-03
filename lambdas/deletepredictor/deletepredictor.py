import actions
from loader import Loader

LOADER = Loader()


def lambda_handler(event, context):
    predictors = event['params']['Predictor']
    try:
        for predictor in predictors:
            LOADER.forecast_cli.delete_predictor(PredictorArn=predictor['PredictorArn'])
        for predictor in predictors:
            actions.take_action_delete(
                LOADER.forecast_cli.describe_predictor(
                    PredictorArn=predictor['PredictorArn']
                )['Status']
            )
        

    except (LOADER.forecast_cli.exceptions.ResourceNotFoundException, KeyError):
        LOADER.logger.info('Predictor not found! Passing.')
    

    return event