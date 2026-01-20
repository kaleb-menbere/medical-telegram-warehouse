from dagster import job, op, get_dagster_logger

logger = get_dagster_logger()

@op
def hello():
    logger.info("👋 Hello from Dagster!")
    return "Hello"

@op 
def process(greeting):
    logger.info(f"🔄 Processing: {greeting}")
    return f"{greeting} World!"

@op
def finish(result):
    logger.info(f"🎉 Final result: {result}")
    return result

@job
def simple_pipeline():
    """A simple test pipeline"""
    greeting = hello()
    processed = process(greeting)
    finish(processed)

if __name__ == "__main__":
    print("🚀 Running simple Dagster test...")
    result = simple_pipeline.execute_in_process()
    print(f"✅ Test completed successfully!")
    # Access output from the finish op
    print(f"Output: {result.output_for_node('finish')}")