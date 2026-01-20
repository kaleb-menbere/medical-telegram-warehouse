from dagster import job, op, get_dagster_logger
import time

logger = get_dagster_logger()

@op
def step_one():
    """First step - simulate scraping"""
    logger.info("✅ Step 1: Simulating Telegram scraping...")
    time.sleep(1)
    return {"status": "success", "message": "Scraping completed"}

@op
def step_two(step_one_result):
    """Second step - simulate loading"""
    logger.info("✅ Step 2: Simulating data loading...")
    time.sleep(1)
    return {"status": "success", "message": "Loading completed", "step_one": step_one_result}

@op
def step_three(step_two_result):
    """Third step - simulate transformation"""
    logger.info("✅ Step 3: Simulating dbt transformations...")
    time.sleep(1)
    return {"status": "success", "message": "Transformations completed", "step_two": step_two_result}

@op
def step_four(step_three_result):
    """Fourth step - simulate YOLO enrichment"""
    logger.info("✅ Step 4: Simulating YOLO enrichment...")
    time.sleep(1)
    return {"status": "success", "message": "Enrichment completed", "step_three": step_three_result}

@op
def step_five(step_four_result):
    """Fifth step - final verification"""
    logger.info("✅ Step 5: Simulating verification...")
    time.sleep(1)
    final_result = {
        "status": "success",
        "message": "Pipeline execution completed successfully!",
        "all_steps": step_four_result,
        "timestamp": time.time()
    }
    logger.info(f"🎉 {final_result['message']}")
    return final_result

@job
def test_pipeline():
    """Simplified test pipeline"""
    step1 = step_one()
    step2 = step_two(step1)
    step3 = step_three(step2)
    step4 = step_four(step3)
    result = step_five(step4)
    return result

if __name__ == "__main__":
    # Run the test pipeline directly
    print("🚀 Running test pipeline...")
    result = test_pipeline.execute_in_process()
    print(f"✅ Test completed: {result.success}")