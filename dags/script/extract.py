import os
import shutil

BASE_DATA_PATH = "/opt/airflow/data"

LANDING_DIR = f"{BASE_DATA_PATH}/Landing_zone"
RAW_DIR = f"{BASE_DATA_PATH}/Raw_zone"
SERVING_DIR = f"{BASE_DATA_PATH}/Serving_zone"


def extract():
    source_dir = LANDING_DIR
    dest_dir = RAW_DIR

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]

    for file in files:
        shutil.copyfile(os.path.join(source_dir, file), os.path.join(dest_dir, file))
        print(f"Extracted files from {source_dir} to {dest_dir} {len(files)},files : {files} ")

if __name__ == "__main__":
    extract()