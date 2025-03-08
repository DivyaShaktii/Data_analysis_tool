import sys
import pandas as pd
import os

if len(sys.argv) < 3:
    print("Error: Input file path and output directory required")
    sys.exit(1)

file_path = sys.argv[1]
output_dir = sys.argv[2]

print(f"Input file path: {file_path}")
print(f"Output directory: {output_dir}")

try:
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.endswith(".xlsx"):
        df = pd.read_excel(file_path)
    else:
        print("Error: Unsupported file format")
        sys.exit(1)

    df.columns = [col.upper() for col in df.columns]

    os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists

    # Create the output file path inside the mounted output directory
    output_filename = os.path.basename(file_path).replace(".", "_processed.")
    output_path = os.path.join(output_dir, output_filename)

    print(f"Saving transformed file to: {output_path}")

    df.to_csv(output_path, index=False)

    print(f"Processed file saved to: {output_path}")

except Exception as e:
    print(f"Error processing file: {str(e)}")
    sys.exit(1)
