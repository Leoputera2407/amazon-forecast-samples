from os import environ
import actions
from loader import Loader

ARN = 'arn:aws:forecast:{region}:{account}:predictor/{name}'
LOADER = Loader()


def lambda_handler(event, context):
    status = None
    params = event['params']
    predictors = event['params']['Predictor']
    arns={}
    for predictor in predictors:
        predictor_name = predictor['PredictorName']
        arns[predictor_name] = ARN.format(
            account=params['misc']['AccountID'],
            name=predictor_name,
            region=environ['AWS_REGION']
        )
    try:
        status =[]
        for arn in arns.values():
            stat = LOADER.forecast_cli.describe_predictor(
                PredictorArn=arn
            )
            status.append(stat['Status'])

    except LOADER.forecast_cli.exceptions.ResourceNotFoundException:
        LOADER.logger.info(
            'Predictor not found! Will follow to create new predictor.'
        )
        dataset_group = event['params']['DatasetGroup']
        status=[]
        for predictor in predictors:
            predictor['InputDataConfig'] = {
                'DatasetGroupArn': dataset_group['DatasetGroupArn']
            }
            LOADER.forecast_cli.create_predictor(**predictor)
            predictor_name = predictor['PredictorName']
            stat = LOADER.forecast_cli.describe_predictor(
                PredictorArn=arns[predictor_name]
            )
            status.append(stat['Status'])
    for idx,predictor in enumerate(predictors):
        event['params']['Predictor'][idx]['PredictorArn'] = arns[predictor['PredictorName']]       
    actions.take_action(status)
    return event