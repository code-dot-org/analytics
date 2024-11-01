import boto3
import psycopg2

# S3 and Redshift configuration
s3_bucket = 'your-s3-bucket-name'
redshift_table = 'your_redshift_table'
redshift_iam_role = 'arn:aws:iam::your-account-id:role/RedshiftCopyRole'
redshift_host = 'your-redshift-cluster-url'
redshift_dbname = 'your_dbname'
redshift_user = 'your_username'
redshift_password = 'your_password'
redshift_port = 5439

def upload_to_s3(file_name, bucket, key):
    """Uploads a file to S3."""
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, key)
        s3_path = f"s3://{bucket}/{key}"
        print(f"File uploaded to {s3_path}")
        return s3_path
    except Exception as e:
        print(f"An error occurred while uploading to S3: {e}")
        return None

def load_to_redshift(s3_path, table, iam_role):
    """Loads a CSV file from S3 into a Redshift table."""
    conn = psycopg2.connect(
        dbname=redshift_dbname,
        user=redshift_user,
        password=redshift_password,
        port=redshift_port,
        host=redshift_host
    )
    cursor = conn.cursor()
    try:
        query = f"""
        COPY {table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS CSV
        IGNOREHEADER 1;
        """
        cursor.execute(query)
        conn.commit()
        print(f"Data loaded into Redshift table {table}")
    except Exception as e:
        print(f"An error occurred while loading data to Redshift: {e}")
    finally:
        cursor.close()
        conn.close()

# Main execution function
def main(file_name, s3_key):
    # Step 1: Upload file to S3
    s3_path = upload_to_s3(file_name, s3_bucket, s3_key)
    if s3_path:
        # Step 2: Load data from S3 to Redshift
        load_to_redshift(s3_path, redshift_table, redshift_iam_role)

# Run the script with your file and S3 key
file_path = 'path/to/your_file.csv'  # Replace with your local file path
s3_key = 'path/in/bucket/your_file.csv'  # Replace with the desired S3 key path
main(file_path, s3_key)