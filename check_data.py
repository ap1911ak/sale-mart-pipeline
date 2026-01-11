import pandas as pd
import os

def check_data():
    source_dir = 'Raw_zone'
    files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]

    print(f"Checking data in {source_dir}, files found: {files}")
    for file in files:
        file_path = os.path.join(source_dir, file)
        try:
            df = pd.read_csv(file_path)
            print(f"🔍 [Checking File: {file}]")

            # Missing data check
            missing_count = df.isnull().sum()
            if missing_count.sum() > 0:
                print(f"⚠️ Missing data found in {file}:\n{missing_count[missing_count > 0]}")
            else:
                print(f"✅ No missing data in {file}")
            
            #Duplicate data check
            dup_count = df.duplicated().sum()
            if dup_count > 0:
                print(f"⚠️ Duplicate rows found in {file}: {dup_count} duplicates")
            else:
                print(f"✅ No duplicate rows in {file}")

            #Numeric data validation
            numeric_cols = df.select_dtypes(include=['number']).columns
            if not numeric_cols.empty:
                for col in numeric_cols:
                    # negative data check
                    if (df[col] < 0).any():
                        print(f"⚠️ Negative values found in numeric column '{col}' of file {file}")
                    else:
                        print(f"✅ All values in numeric column '{col}' of file {file} are valid")

                    # outlier detection using IQR
                    mean_val = df[col].mean()
                    max_val = df[col].max()
                    if max_val > 10 * mean_val:
                        print(f"⚠️ Potential outliers detected in column '{col}' of file {file} (max: {max_val}, mean: {mean_val})")
                    else:
                        print(f"✅ No significant outliers in column '{col}' of file {file}")
        except Exception as e:
            print(f"Error reading {file}: {e}")

if __name__ == "__main__":
    check_data()