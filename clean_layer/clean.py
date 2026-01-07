
import logging
import boto3
from clean_layer.clean_func import clean_address,clean_counterparty,clean_currency,clean_department,clean_design,clean_payment,clean_payment_type,clean_purchase_order,clean_sales_order,clean_staff,clean_transcation
from clean_layer.star_schema_tables import dim_counterparty,dim_currency,dim_date,dim_design,dim_location,dim_payment_type,dim_staff,dim_transaction,fact_payment,fact_purchase_order,fact_sales_order
import pandas as pd
from clean_layer.utils.save_df_into_parquet import save_data
from clean_layer.utils.get_df import get_df
import os

clean_func_map = {'counterparty':clean_counterparty.clean_counterparty,
 'address':clean_address.clean_address,
 'department':clean_department.clean_department,
 'purchase_order':clean_purchase_order.clean_purchase_order,
 'staff':clean_staff.clean_staff,
 'payment_type':clean_payment_type.clean_payment_type,
 'payment':clean_payment.clean_payment,
 'transaction':clean_transcation.clean_transcation,
 'design':clean_design.clean_design,
 'sales_order':clean_sales_order.clean_sales_order,
 'currency':clean_currency.clean_currency}

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# logging.basicConfig(encoding='utf-8', level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')
raw_bucket_name = os.environ["S3_RAW_BUCKET_NAME"]
processed_bucket_name = os.environ["S3_PROCESSED_BUCKET_NAME"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(encoding='utf-8', level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')


def lambda_handler(event, context):
    logger.info(f"lambda triggered event:{event}, context:{context}")

    # processed_bucket_name = 'test_processed_bucket'
    # raw_bucket_name = 'test_raw_bucket'

    #inital build:
    #check anything in processed bucket,if empty:
    #iterate the raw data bucket, for every file do initial build
    cleaned_df_dict ={}
    s3_client = boto3.client("s3")
    processed_objects = s3_client.list_objects_v2(Bucket=processed_bucket_name)

    if not processed_objects['KeyCount']:
        try:
            logger.info(f"no file found, start init transform")
            tables = list(clean_func_map.keys())
            for prefix in tables:
                start_string = prefix + '/year='
                raw_objects = s3_client.list_objects_v2(Bucket=raw_bucket_name,Prefix = start_string)
                list_keys = [content['Key'] for content in raw_objects['Contents']]
                logger.info(f"Start clean {prefix} table process.")
                base_df = None
                for key in list_keys:
                    start_string = prefix + '/year='
                    if key.startswith(start_string):

                        df = clean_func_map[prefix](file_path = key,bucket_name = raw_bucket_name)
                        if not isinstance(base_df, pd.DataFrame):
                            base_df =df
                        else:
                            base_df = pd.concat([base_df,df], axis=0, ignore_index=True)
                
                cleaned_df_dict[prefix] = base_df
                logger.info(f"Finish clean {prefix} table process.")
        except Exception as e:
            logger.error(f"Major error on stage 1: %s", str(e))
            # print("ERROR IN LAMBDA:", str(e))
            raise


        print(cleaned_df_dict['staff'])

        logger.info("Start counterparty ingest")
        dim_counterparty_df = dim_counterparty.create_dim_counterparty(address_df=cleaned_df_dict['address'],counterparty_df=cleaned_df_dict['counterparty'])
        key = 'dim_counterparty.parquet'
        save_data(dim_counterparty_df,processed_bucket_name,key)
        logger.info("Finish counterparty ingest")

        dim_currency_df = dim_currency.create_dim_currency(cleaned_df_dict['currency'])
        key = 'dim_currency.parquet'
        save_data(dim_currency_df,processed_bucket_name,key)

        dim_date_df = dim_date.create_dim_date()
        key = 'dim_date.parquet'
        save_data(dim_date_df,processed_bucket_name,key)

        logger.info("Start design part")
        dim_design_df = dim_design.create_dim_design(cleaned_df_dict['design'])
        key = 'dim_design.parquet'
        save_data(dim_design_df,processed_bucket_name,key)
        logger.info("Finish design part")

        try:
            logger.info("Start location part")
            logger.info("dataframe head - {}".format(cleaned_df_dict['address'].head()))
            logger.info(f"{str(cleaned_df_dict['address'])}")
            dim_location_df = dim_location.create_dim_location(cleaned_df_dict['address'])
            key = 'dim_location.parquet'
            save_data(dim_location_df,processed_bucket_name,key)
        except Exception as e:
            logger.error(f"Major error on location transform: %s", str(e))
            # print("ERROR IN LAMBDA:", str(e))
            raise

        dim_payment_type_df = dim_payment_type.create_dim_payment_type(cleaned_df_dict['payment_type'])
        key = 'dim_payment_type.parquet'
        save_data(dim_payment_type_df,processed_bucket_name,key)

        logger.info("Start staff part")
        logger.info(f"{str(cleaned_df_dict['staff'])}")
        # logger.info("dataframe staff head - {}".format(cleaned_df_dict['staff'].head()))

        dim_staff_df = dim_staff.create_dim_staff(cleaned_df_dict['staff'],cleaned_df_dict['department'])
        key = 'dim_staff.parquet'
        save_data(dim_staff_df,processed_bucket_name,key)
        logger.info("Finish staff part")

        cleaned_department_df = cleaned_df_dict['department']
        key = 'cleaned_department_df.parquet'
        save_data(cleaned_department_df,processed_bucket_name,key)


        logger.info("Transaction part")
        logger.info("dataframe transaction head - {}".format(cleaned_df_dict['transaction'].head()))
        logger.info(f"{str(cleaned_df_dict['transaction'])}")

        dim_transaction_df = dim_transaction.create_dim_transaction(cleaned_df_dict['transaction'])
        key = 'dim_transaction.parquet'
        save_data(dim_transaction_df,processed_bucket_name,key)

        fact_payment_df = fact_payment.create_fact_payment(payment = cleaned_df_dict['payment'], dim_payment_type = dim_payment_type_df, dim_transaction = dim_transaction_df, dim_counterparty = dim_counterparty_df , dim_currency = dim_currency_df , dim_date = dim_date_df )
        key = 'fact_payment.parquet'
        save_data(fact_payment_df,processed_bucket_name,key)

        fact_purchase_order_df = fact_purchase_order.create_fact_purchase_order(dim_date_df = dim_date_df,
                               dim_currency_df = dim_currency_df,
                               dim_staff_df = dim_staff_df,
                               dim_counterparty_df = dim_counterparty_df,
                               dim_location_df = dim_location_df,
                               purchase_order_df = cleaned_df_dict['purchase_order'])

        key = 'fact_purchase_order.parquet'
        save_data(fact_purchase_order_df,processed_bucket_name,key)

        fact_sales_order_df = fact_sales_order.create_fact_sales_order(
                                sales_order = cleaned_df_dict['sales_order'],
                                dim_date =  dim_date_df,
                                dim_staff = dim_staff_df,
                                dim_counterparty = dim_counterparty_df,
                                dim_currency = dim_currency_df,
                                dim_design = dim_design_df,
                                dim_location = dim_location_df )
        key = 'fact_sales_order.parquet'
        save_data(fact_sales_order_df,processed_bucket_name,key)
        logger.info("Initial build done!")

    # else:
    #     try:
    #         key = event['Records'][0]['s3']['object']['key']
    #         tables = list(clean_func_map.keys())

    #         def update_dim(df,processed_bucket_name,create_func,key):
    #             dim_df =  get_df(processed_bucket_name,key)
    #             df = create_func(df)
    #             new_df = pd.concat([dim_df,df],axis=0, ignore_index=True)
    #             save_data(new_df,processed_bucket_name,key)



    #         dim_func_map = {
    #                     'address': [dim_location.create_dim_location,'dim_location.parquet'],
    #                     'payment_type': [dim_payment_type.create_dim_payment_type,'dim_payment_type.parquet'],
    #                     'transaction': [dim_transaction.create_dim_transaction,'dim_transaction.parquet'],
    #                     'design': [dim_design.create_dim_design,'dim_design.parquet'],
    #                     'currency':[dim_currency.create_dim_currency,'dim_currency.parquet']}

    #         for prefix in tables:
    #             start_string = prefix + '/year='
    #             if key.startswith(start_string):
    #                 df = clean_func_map[prefix](file_path = key,bucket_name = raw_bucket_name)
    #                 if prefix in list(dim_func_map.keys()):
    #                     update_dim(df,processed_bucket_name,dim_func_map[prefix][0],dim_func_map[prefix][1])

    #                 elif prefix=='counterparty':
    #                     dim_counterparty.update_counterparty(df,processed_bucket_name)

    #                 elif prefix == 'staff':
    #                     dim_staff.update_dim_staff(df,processed_bucket_name)

    #                 elif prefix == 'department':
    #                     dim_staff.update_dim_staff(df,processed_bucket_name)

    # #fact table
    #                 elif prefix == 'sales_order':
    #                     #get all the dim table use get_df
    #                     #get old fact use get_df
    #                     #using the dim tables to make
    #                     # using fact_sales_order.create_fact_sales_order generate new table
    #                     # pd.concat([dim_df,df],axis=0, ignore_index=True)
    #                     dim_counterparty_df =  get_df(processed_bucket_name,'dim_counterparty.parquet')
    #                     dim_currency_df = get_df(processed_bucket_name,'dim_currency.parquet')
    #                     dim_date_df = get_df(processed_bucket_name,'dim_date.parquet')
    #                     dim_design_df = get_df(processed_bucket_name,'dim_design.parquet')
    #                     dim_location_df = get_df(processed_bucket_name,'dim_location.parquet')
    #                     dim_payment_type_df = get_df(processed_bucket_name,'dim_payment_type.parquet')
    #                     dim_staff_df = get_df(processed_bucket_name,'dim_staff.parquet')
    #                     dim_transaction_df = get_df(processed_bucket_name,'dim_transaction.parquet')
    #                     key = 'fact_sales_order.parquet'
    #                     dim_df =  get_df(processed_bucket_name,key)
    #                     df = fact_sales_order.create_fact_sales_order(
    #                                 sales_order = df,
    #                                 dim_date_df = dim_date_df,
    #                                 dim_staff = dim_staff_df,
    #                                 dim_counterparty = dim_counterparty_df,
    #                                 dim_currency = dim_currency_df,
    #                                 dim_design = dim_design_df,
    #                                 dim_location = dim_location_df )
    #                     new_df = pd.concat([dim_df,df],axis=0, ignore_index=True)
    #                     save_data(new_df,processed_bucket_name,key)

    #                 elif prefix == 'purchase_order':
    #                     dim_counterparty_df =  get_df(processed_bucket_name,'dim_counterparty.parquet')
    #                     dim_currency_df = get_df(processed_bucket_name,'dim_currency.parquet')
    #                     dim_date_df = get_df(processed_bucket_name,'dim_date.parquet')
    #                     dim_design_df = get_df(processed_bucket_name,'dim_design.parquet')
    #                     dim_location_df = get_df(processed_bucket_name,'dim_location.parquet')
    #                     dim_payment_type_df = get_df(processed_bucket_name,'dim_payment_type.parquet')
    #                     dim_staff_df = get_df(processed_bucket_name,'dim_staff.parquet')
    #                     dim_transaction_df = get_df(processed_bucket_name,'dim_transaction.parquet')
    #                     key = 'fact_purchase_order.parquet'
    #                     dim_df =  get_df(processed_bucket_name,key)
    #                     df = fact_purchase_order.create_fact_purchase_order(dim_date_df = dim_date_df,
    #                             dim_currency_df = dim_currency_df,
    #                             dim_staff_df = dim_staff_df,
    #                             dim_counterparty_df = dim_counterparty_df,
    #                             dim_location_df = dim_location_df ,
    #                             purchase_order_df = cleaned_df_dict['purchase_order'])

    #                     new_df = pd.concat([dim_df,df],axis=0, ignore_index=True)
    #                     save_data(new_df,processed_bucket_name,key)

    #                 elif prefix == 'payment':
    #                     dim_counterparty_df =  get_df(processed_bucket_name,'dim_counterparty.parquet')
    #                     dim_currency_df = get_df(processed_bucket_name,'dim_currency.parquet')
    #                     dim_date_df = get_df(processed_bucket_name,'dim_date.parquet')
    #                     dim_design_df = get_df(processed_bucket_name,'dim_design.parquet')
    #                     dim_location_df = get_df(processed_bucket_name,'dim_location.parquet')
    #                     dim_payment_type_df = get_df(processed_bucket_name,'dim_payment_type.parquet')
    #                     dim_staff_df = get_df(processed_bucket_name,'dim_staff.parquet')
    #                     dim_transaction_df = get_df(processed_bucket_name,'dim_transaction.parquet')
    #                     key = 'fact_payment.parquet'
    #                     dim_df =  get_df(processed_bucket_name,key)
    #                     df = fact_payment.create_fact_payment(payment = cleaned_df_dict['payment'], dim_payment_type = dim_payment_type_df, dim_transaction = dim_transaction_df, dim_counterparty = dim_counterparty_df , dim_currency = dim_currency_df , dim_date = dim_date_df)
    #                     new_df = pd.concat([dim_df,df],axis=0, ignore_index=True)
    #                     save_data(new_df,processed_bucket_name,key)
    #     except Exception as e:
    #         logger.error(f"Major error on stage 2: %s", str(e))
    #         # print("ERROR IN LAMBDA:", str(e))
    #         raise
