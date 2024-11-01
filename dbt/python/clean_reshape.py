import pandas as pd
import re

# File paths
input_file = 'your_input_file.csv'
output_file = 'your_output_file.csv'

# Columns to keep in snake_case
columns_to_keep = [
    "col_1", 
    "col_2", 
    "col_3",
    "col_4"
]

def clean_column_names(columns):
    """Remove extra quotes, whitespace, special characters, and convert to snake_case."""
    cleaned_columns = []
    for col in columns:
        clean_col = re.sub(r'["\'!@#$%^&*()+=]', '', col).strip()
        snake_case_col = re.sub(r'\s+', '_', clean_col).lower()
        cleaned_columns.append(snake_case_col)
    return cleaned_columns

def transform_csv(input_file, output_file, columns_to_keep):
    try:
        # Step 1: Try reading the CSV with default parameters
        try:
            df = pd.read_csv(input_file, encoding='utf-8-sig', on_bad_lines='skip')
        except pd.errors.ParserError:
            # Step 2: If there’s still an issue, specify the delimiter as comma
            df = pd.read_csv(input_file, delimiter=',', encoding='utf-8-sig', on_bad_lines='skip')
        
        # Clean the column names
        df.columns = clean_column_names(df.columns)

        # Print cleaned snake_case column names for verification
        print("Cleaned snake_case columns:", df.columns.tolist())

        # Select only available columns in `columns_to_keep`
        available_columns = [col for col in columns_to_keep if col in df.columns]
        print("Available columns to keep:", available_columns)

        if not available_columns:
            print("No matching columns found. Check the `columns_to_keep` list and column names in the file.")
            return

        # Filter the DataFrame to only keep available columns
        df = df[available_columns]

        # Confirm there is data after selecting columns
        print("DataFrame preview after selecting columns:")
        print(df.head())
        print("Number of rows in the filtered DataFrame:", len(df))

        # Convert all values to lowercase
        df = df.apply(lambda x: x.str.lower() if x.dtype == "object" else x)

        # Save to new CSV file
        if not df.empty:
            df.to_csv(output_file, index=False)
            print(f"File saved to {output_file}")
        else:
            print("DataFrame is empty after filtering. No file saved.")

    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Run the function
transform_csv(input_file, output_file, columns_to_keep)