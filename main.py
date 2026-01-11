import logging
import extract
import clean_data as clean
import load_data as load
from datetime import datetime

# Log settings
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_pipeline():
    start_time = datetime.now()
    logging.info("--- Data Pipeline Started ---")
    
    try:
        # Step 1: Extract
        print("Executing: Extraction...")
        extract.extract()
        logging.info("Extraction Step: SUCCESS")
        
        # Step 2: Transform/Clean
        print("Executing: Cleaning & Feature Engineering...")
        clean.clean_data()
        logging.info("Cleaning Step: SUCCESS")
        
        # Step 3: Load
        print("Executing: Loading to Serving Zone...")
        load.load_data()
        logging.info("Loading Step: SUCCESS")
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"Pipeline finished successfully in {duration}!")
        logging.info(f"--- Pipeline Finished Successfully (Duration: {duration}) ---")

    except Exception as e:
        error_msg = f"Pipeline FAILED at some stage: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

if __name__ == "__main__":
    run_pipeline()