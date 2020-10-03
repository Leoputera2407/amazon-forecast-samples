from os import environ
from boto3 import client
import actions
from loader import Loader
import json

CLOUDWATCH_CLI = client('cloudwatch')
ARN = 'arn:aws:forecast:{region}:{account}:forecast/{name}'
JOB_ARN = 'arn:aws:forecast:{region}:{account}:forecast-export-job/' \
          '{name}/{name}_{date}'
LOADER = Loader()


# Post training accuracy metrics from the previous step (predictor) to CloudWatch
def post_metric(metrics):
    # print(dumps(metrics))
    for metric in metrics['PredictorEvaluationResults']:
        CLOUDWATCH_CLI.put_metric_data(
            Namespace='FORECAST',
            MetricData=[
                {
                    'Dimensions':
                        [
                            {
                                'Name': 'Algorithm',
                                'Value': metric['AlgorithmArn']
                            }, {
                                'Name': 'Quantile',
                                'Value': str(quantile['Quantile'])
                            }
                        ],
                    'MetricName': 'WQL',
                    'Unit': 'None',
                    'Value': quantile['LossValue']
                } for quantile in metric['TestWindows'][0]['Metrics']
                ['WeightedQuantileLosses']
            ] + [
                {
                    'Dimensions':
                        [
                            {
                                'Name': 'Algorithm',
                                'Value': metric['AlgorithmArn']
                            }
                        ],
                    'MetricName': 'RMSE',
                    'Unit': 'None',
                    'Value': metric['TestWindows'][0]['Metrics']['RMSE']
                }
            ]
        )

def extract_metrics(metrics):
    results = {}
    for metric in metrics['PredictorEvaluationResults']:
        results['rmse'] =  metric['TestWindows'][0]['Metrics']['RMSE']
        for  quantile in metric['TestWindows'][0]['Metrics']['WeightedQuantileLosses']:
            results[str(quantile['Quantile'])] =  quantile['LossValue']
    return(results)


def lambda_handler(event, context):
    forecasts = event['params']['Forecast']
    predictors = event['params']['Predictor']
    status = None
    forecast_obj = {}
    forecast_idx = {}
    forecast_arns ={}
    export_arns={}
    forecast_s3 ={}
    for idx, forecast in enumerate(forecasts):
        predictor_name = forecast['PredictorName']
        forecast_name = forecast['ForecastName']
        curr_date = event['params']['misc']['currentDate']
        forecast_arns[forecast_name] = ARN.format(
            account=event['params']['misc']['AccountID'],
            name= forecast_name,
            region=environ['AWS_REGION']
        )
        export_arns[forecast_name] = JOB_ARN.format(
            account=event['params']['misc']['AccountID'],
            name=forecast_name,
            date=event['params']['misc']['currentDate'],
            region=environ['AWS_REGION']
        )
        forecast_s3[forecast_name] = 's3://{bucket}/{forecast_name}_{date}/'.format(
            bucket=event['params']['misc']['bucket'],
            forecast_name = forecast_name,
            date = curr_date
        )
        forecast_obj[predictor_name] = forecast
        forecast_idx[predictor_name] = idx

    # Creates Forecast and export Predictor metrics if Forecast does not exist yet.
    # Will throw an exception while the forecast is being created.
    try:
        #If resource pending, will re-tried later by Step-Wise.
        #If resource not found, will be excepted and creates and export predictor.
        for arn in forecast_arns.values():
            actions.take_action(
                LOADER.forecast_cli.describe_forecast(
                    ForecastArn=arn
                )['Status']
            )
    except LOADER.forecast_cli.exceptions.ResourceNotFoundException:
        LOADER.logger.info('Forecast not found. Creating new forecast.')
        for predictor in predictors:
            predictor_name = predictor['PredictorName']
            metrics =  LOADER.forecast_cli.get_accuracy_metrics(
                 PredictorArn=predictor['PredictorArn']
                 )
            #Post metrics to cloudwatch
            post_metric(metrics)
            forecast = forecast_obj[predictor_name]
            LOADER.forecast_cli.create_forecast(
                ForecastName=forecast['ForecastName'],
                ForecastTypes=forecast['ForecastTypes'],
                PredictorArn=predictor['PredictorArn']
            )
        for arn in forecast_arns.values():
            actions.take_action(
                LOADER.forecast_cli.describe_forecast(
                    ForecastArn=arn
                )['Status']
            )

    # Creates forecast export job if it does not exist yet. Will trhow an exception
    # while the forecast export job is being created.
    try:
        status=[]
        for export_arn in export_arns.values():
            stat = LOADER.forecast_cli.describe_forecast_export_job(
                ForecastExportJobArn=export_arn
            )
            status.append(stat['Status'])
    except LOADER.forecast_cli.exceptions.ResourceNotFoundException:
        LOADER.logger.info('Forecast export not found. Creating new export.')
        status =[]
        for idx, forecast in enumerate(forecasts):
            forecast_name = forecast['ForecastName']
            curr_date = event['params']['misc']['currentDate']
            LOADER.forecast_cli.create_forecast_export_job(
                ForecastExportJobName='{name}_{date}'.format(
                    name=forecast_name, 
                    date= curr_date
                ),
                ForecastArn=forecast_arns[forecast_name],
                Destination={
                    'S3Config':
                        {
                            'Path':
                                forecast_s3[forecast_name],
                            'RoleArn':
                                environ['EXPORT_ROLE']
                        }
                }
            )
            stat = LOADER.forecast_cli.describe_forecast_export_job(
                ForecastExportJobArn=export_arns[forecast_name]
            )
            status.append(stat['Status'])
    
    #Put results on payload
    for predictor in predictors:
        predictor_name = predictor['PredictorName']
        metrics =  LOADER.forecast_cli.get_accuracy_metrics(
            PredictorArn=predictor['PredictorArn']
        )
        results = extract_metrics(metrics)
        idx = forecast_idx[predictor_name]
        event['params']['Forecast'][idx]['metric'] = results
        
    for idx, forecast in enumerate(forecasts): 
        forecast_name = forecast['ForecastName']
        event['params']['Forecast'][idx]['ForecastArn']  = forecast_arns[forecast_name]
        event['params']['Forecast'][idx]['ForecastExportJobArn'] = export_arns[forecast_name]
        event['params']['Forecast'][idx]['s3path'] = forecast_s3[forecast_name]
    actions.take_action(status)
    return event

