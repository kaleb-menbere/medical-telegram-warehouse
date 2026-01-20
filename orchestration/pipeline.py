import os
import sys
from datetime import datetime
from dagster import job, op, get_dagster_logger, resource, OpExecutionContext
import subprocess
import psycopg2
import pandas as pd

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = get_dagster_logger()

# Resource for database connection
class PostgreSQLResource:
    def __init__(self):
        self.conn_params = {
            "host": "localhost",
            "database": "telegram_warehouse",
            "user": "postgres",
            "password": "password"  # Update with your password
        }
    
    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

@resource
def postgres_resource(init_context):
    return PostgreSQLResource()

@op(required_resource_keys={"postgres"})
def scrape_telegram_data(context: OpExecutionContext):
    """Run Telegram scraper to collect new data"""
    logger.info("🔄 Starting Telegram data scraping...")
    
    try:
        # Simulate scraping (replace with actual scraper)
        logger.info("✅ Simulated: Telegram scraping completed")
        return {"status": "success", "message": "Scraping simulated"}
            
    except Exception as e:
        logger.error(f"❌ Error in scraping: {str(e)}")
        raise

@op(required_resource_keys={"postgres"})
def load_raw_to_postgres(context: OpExecutionContext, scrape_result):
    """Load scraped JSON data into PostgreSQL raw table"""
    logger.info("📥 Loading raw data to PostgreSQL...")
    
    try:
        # Simulate loading
        logger.info("✅ Simulated: Data loaded to PostgreSQL")
        return {"status": "success", "message": "Loading simulated", "step_one": scrape_result}
            
    except Exception as e:
        logger.error(f"❌ Error in data loading: {str(e)}")
        raise

@op(required_resource_keys={"postgres"})
def run_dbt_transformations(context: OpExecutionContext, load_result):
    """Run dbt models to transform raw data into data warehouse"""
    logger.info("🔄 Running dbt transformations...")
    
    try:
        # Simulate dbt run
        logger.info("✅ Simulated: dbt transformations completed")
        return {"status": "success", "message": "dbt transformations simulated", "step_two": load_result}
            
    except Exception as e:
        logger.error(f"❌ Error in dbt transformations: {str(e)}")
        raise

@op(required_resource_keys={"postgres"})
def run_yolo_enrichment(context: OpExecutionContext, dbt_result):
    """Run YOLO object detection on new images"""
    logger.info("🖼️ Running YOLO object detection...")
    
    try:
        # Simulate YOLO
        logger.info("✅ Simulated: YOLO enrichment completed")
        return {
            "status": "success",
            "message": "YOLO enrichment simulated",
            "step_three": dbt_result
        }
            
    except Exception as e:
        logger.error(f"❌ Error in YOLO enrichment: {str(e)}")
        raise

@op(required_resource_keys={"postgres"})
def verify_pipeline_results(context: OpExecutionContext, yolo_result):
    """Verify pipeline results and log summary"""
    logger.info("📊 Verifying pipeline results...")
    
    try:
        # Simulate verification
        logger.info("✅ Simulated: Pipeline verification completed")
        
        final_result = {
            "status": "success",
            "message": "Full pipeline execution simulated successfully!",
            "all_steps": yolo_result,
            "timestamp": datetime.now().isoformat(),
            "pipeline": "telegram_data_pipeline"
        }
        
        logger.info(f"🎉 {final_result['message']}")
        logger.info(f"📅 Completed at: {final_result['timestamp']}")
        
        return final_result
        
    except Exception as e:
        logger.error(f"❌ Error in verification: {str(e)}")
        raise

@job(resource_defs={"postgres": postgres_resource})
def telegram_data_pipeline():
    """Main pipeline: Scrape → Load → Transform → Enrich → Verify"""
    
    # Define execution graph - NO RETURN STATEMENT
    scrape_result = scrape_telegram_data()
    load_result = load_raw_to_postgres(scrape_result)
    dbt_result = run_dbt_transformations(load_result)
    yolo_result = run_yolo_enrichment(dbt_result)
    verify_pipeline_results(yolo_result)

# For direct execution
if __name__ == "__main__":
    print("🚀 Running Telegram Data Pipeline (simulated)...")
    result = telegram_data_pipeline.execute_in_process()
    print(f"✅ Pipeline executed: {result.success}")