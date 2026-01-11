import os
import shutil

def extract():
    source_dir = 'Landing_zone'
    dest_dir = 'Raw_zone'

    if not os.path.exists(_dir):
        os.makedirs(dest_dir)

    files = [f  for f in os.listdir(source_dir) if f.endswith('.csv')]

    for file in files:
        shutil.copy(os.path.join(source_dir, file), os.path.join(dest_dir, file))
        print(f"Extracted files from {source_dir} to {dest_dir} {len(files)},files : {files} ")
if __name__ == "__main__":
    extract()    