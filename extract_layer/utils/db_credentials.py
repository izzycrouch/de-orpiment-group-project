import boto3
from botocore.exceptions import ClientError
import json

def get_db_credentials(secret_name = "project-orpiment-raw-database", region_name = "eu-west-2"):
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    dict_secret = json.loads(secret)
    return dict_secret
