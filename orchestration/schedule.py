from dagster import schedule
from .pipeline import telegram_data_pipeline

@schedule(
    cron_schedule="0 2 * * *",  # Run daily at 2 AM
    job=telegram_data_pipeline,
    execution_timezone="UTC",
)
def daily_pipeline_schedule(context):
    """Schedule the pipeline to run daily"""
    return {}