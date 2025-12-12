import boto3
import zipfile
import io

#event = {'bucket': 'bucket_name'}

def lambda_handler(event: dict):
    s3_client = boto3.client('s3')

    bucket = "lambda-func-code-aci"

    response = s3_client.list_objects_v2(Bucket=bucket)

    try:
        contents = response['Contents']

        non_zip_contents = [obj for obj in contents if not obj['Key'].endswith('.zip')]

        loose_objects = [obj for obj in non_zip_contents if '/' not in obj["Key"]]

        for obj in loose_objects:

            key = obj['Key']

            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()

            zip_file_name = obj['Key'].replace('.py', '.zip')

            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.writestr(key, content)

            zip_buffer.seek(0)
            s3_client.upload_fileobj(zip_buffer, bucket, zip_file_name)


        folders = {}

        for obj in non_zip_contents:
            key = obj['Key']
            if '/' in key:
                split_keys = key.split('/')
                folder = split_keys[0]

                if folder not in folders:
                    folders[folder] = []

                folders[folder].append(key)


        for folder, keys in folders.items():
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for key in keys:
                    s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
                    content = s3_obj["Body"].read()

                    filename = key.split("/")[1]
                    zipf.writestr(filename, content)

            zip_buffer.seek(0)

            zip_name = f"{folder}.zip"

            s3_client.upload_fileobj(zip_buffer, bucket, zip_name)

    except:
        raise Exception