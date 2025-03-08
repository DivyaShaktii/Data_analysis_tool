exec("pip install pandas")
import pandas as pd
import json

# Determine file type and read accordingly
import os
file_extension = os.path.splitext('/data/input_file*')[1].lower()

if file_extension in ['.csv']:
    df = pd.read_csv('/data/input_file.csv')
elif file_extension in ['.xlsx', '.xls']:
    df = pd.read_excel('/data/input_file' + file_extension)
else:
    raise ValueError(f"Unsupported file format: {file_extension}")

# Process your data here
# Example: Calculate summary statistics
result = {
    'row_count': len(df),
    'column_count': len(df.columns),
    'columns': list(df.columns),
    'summary': df.describe().to_dict()
}

# Save results
# Option 1: Save as JSON
with open('/data/output/result.json', 'w') as f:
    json.dump(result, f)

# Option 2: Save as CSV
df.to_csv('/data/output/processed_data.csv', index=False)

# Option 3: Save as Excel
df.to_excel('/data/output/processed_data.xlsx', index=False)

print("Processing completed successfully")