import json
import psycopg2
import pyarrow.parquet as pq
from io import BytesIO
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Extract information from the S3 event
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
        
        s3_bucket = event['s3_bucket']
        s3_key = event['s3_key']

        # Read data from S3
        try:
            s3_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            data = s3_object['Body'].read()
            
            parquet_data = pq.read_table(BytesIO(data)) 
            logger.info("read processed data from bucket")
        except Exception as e:
            logger.error("MAJOR_ERROR: %s", str(e))
            print(f"Error reading data from S3: {str(e)}")
            raise
        
        table_name = determine_target_table(s3_key)

        # Load data into PostgreSQL
        try:
            # Your PostgreSQL connection parameters
            # db_params = {
            #     'dbname': 'postgres',
            #     'user': 'postgres',
            #     'password': 'postgres',
            #     'host': 'myretailinstance.ctn92obk1nsu.ap-southeast-2.rds.amazonaws.com',
            #     'port': '5432'
            # }

            # Connect to PostgreSQL
            # conn = psycopg2.connect(**db_params)

            # Create a cursor
            # cursor = conn.cursor()

            # Assuming parquet_data is your PyArrow Table
            data_values = []
            
            # Iterate over columns
            for row_index in range(parquet_data.num_rows):
                # Extract values from each column for the current row
                row_data = [column[row_index].as_py() for column in parquet_data.itercolumns()]
        
                # Append the tuple to the data_values list
                data_values.append(tuple(row_data))
                
            # Insert data into PostgreSQL
            if table_name == 'dim_counterparty':
                insert_query = f"INSERT INTO {table_name} (counterparty_legal_name,\
                    counterparty_legal_address_line_1, counterparty_legal_address_line_2,\
                    counterparty_legal_district, counterparty_legal_city, counterparty_legal_postal_code,\
                    counterparty_legal_country, counterparty_legal_phone_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
            elif table_name == 'dim_currency':
                insert_query = f"INSERT INTO {table_name} (currency_code, currency_name) VALUES (%s, %s);"
            elif table_name == 'dim_date':
                insert_query = f"INSERT INTO {table_name} (year, month, day, day_of_week, day_name, month_name, quarter) VALUES (%s, %s, %s, %s, %s, %s, %s);"
            elif table_name == 'dim_design':
                insert_query = f"INSERT INTO {table_name} (design_name, file_location, file_name) VALUES (%s, %s, %s);"
            elif table_name == 'dim_location':
                insert_query = f"INSERT INTO {table_name} (address_line_1, address_line_2,\
                    district, city, postal_code, country, phone) VALUES (%s, %s, %s, %s, %s, %s, %s);"
            elif table_name == 'dim_payment_type':
                insert_query = f"INSERT INTO {table_name} (payment_type_name) VALUES (%s);"
            elif table_name == 'dim_staff':
                insert_query = f"INSERT INTO {table_name} (first_name, last_name, department_name,\
                    location, email_address) VALUES (%s, %s, %s, %s, %s);"
            elif table_name == 'dim_transaction':
                insert_query = f"INSERT INTO {table_name} (transaction_type, sales_order_id, purchase_order_id) VALUES (%s, %s, %s);"
            elif table_name == 'fact_payment':
                insert_query = f"INSERT INTO {table_name} (payment_id, created_date, created_time,\
                    last_updated_date, last_updated_time, transaction_id,\
                    counterparty_id, payment_amount, currency_id, payment_type_id,\
                    paid, payment_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            elif table_name == 'fact_purchase_order':
                insert_query = f"INSERT INTO {table_name} (purchase_order_id, created_date, created_time, last_updated_date,\
                    last_updated_time, staff_id, counterparty_id, item_code,\
                    item_quantity, item_unit_price, currency_id, agreed_delivery_date,\
                    agreed_payment_date, agreed_delivery_location_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            elif table_name == 'fact_sales_order':
                insert_query = f"INSERT INTO {table_name} (sales_order_id, created_date, created_time,\
                    last_updated_date, last_updated_time, sales_staff_id, counterparty_id,\
                    units_sold, unit_price, currency_id, design_id, agreed_payment_date,\
                    agreed_delivery_date, agreed_delivery_location_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            
            
            cursor.executemany(insert_query, (data_values))  # Replace 'column_name' with your actual column name

            # Commit and close the connection
            conn.commit()
            conn.close()
            logger.info("Data loaded into db!")
        except Exception as e:
            logger.error("MAJOR_ERROR %s", str(e))
            print(f"Error loading data to db: {str(e)}")
            raise 
        
        logger.info("Lambda executed successfully!")
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")


def determine_target_table(filename):
    # Implement your logic to determine the target table based on the Parquet file name
    if "dim_counterparty" in filename:
        return "dim_counterparty"
    elif "dim_currency" in filename:
        return "dim_currency"
    elif "dim_date" in filename:
        return "dim_date"
    elif "dim_design" in filename:
        return "dim_design"
    elif "dim_location" in filename:
        return "dim_location"
    elif "dim_payment_type" in filename:
        return "dim_payment_type"
    elif "dim_staff" in filename:
        return "dim_staff"
    elif "dim_transaction" in filename:
        return "dim_transaction"
    elif "fact_payment" in filename:
        return "fact_payment"
    elif "fact_purchase_order" in filename:
        return "fact_purchase_order"
    elif "fact_sales_order" in filename:
        return "fact_sales_order"