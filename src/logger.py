import logging
import os
from datetime import datetime

# Create a timestamped log file name
LOG_FILE_NAME = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"

# Define the logs directory path
logs_dir = os.path.join(os.getcwd(), "logs")

# Create the logs directory if it does not exist
os.makedirs(logs_dir, exist_ok=True)

# Define the full path to the log file
LOG_FILE_PATH = os.path.join(logs_dir, LOG_FILE_NAME)

# Configure logging
logging.basicConfig(
    filename=LOG_FILE_PATH,
    format='[%(asctime)s] %(lineno)d %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO  # Corrected to logging.INFO
)



# if __name__ == "__main__":
#     logging.info("Logging has started")