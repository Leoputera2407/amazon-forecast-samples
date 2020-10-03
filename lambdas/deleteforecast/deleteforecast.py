import actions
from loader import Loader

LOADER = Loader()


def lambda_handler(event, context):
    forecasts = event['params']['Forecast']
    # Delete forecast export job
    try:
        for forecast in forecasts:
            LOADER.forecast_cli.delete_forecast_export_job(
                ForecastExportJobArn=forecast['ForecastExportJobArn']
            )
        for forecast in forecasts:
            actions.take_action_delete(
                status=LOADER.forecast_cli.describe_forecast_export_job(
                    ForecastExportJobArn=forecast['ForecastExportJobArn']
                    )['Status']
                )
    except (LOADER.forecast_cli.exceptions.ResourceNotFoundException, KeyError):
        LOADER.logger.info('Forecast export job not found. Passing.')

    # Delete forecast
    try:
        for forecast in forecasts:
            LOADER.forecast_cli.delete_forecast(ForecastArn=forecast['ForecastArn'])
        for forecast in forecasts:
            actions.take_action_delete(
                LOADER.forecast_cli.describe_forecast(
                    ForecastArn=forecast['ForecastArn']
                    )['Status']
                )
    except (LOADER.forecast_cli.exceptions.ResourceNotFoundException, KeyError):
        LOADER.logger.info('Forecast not found. Passing.')

    return event
