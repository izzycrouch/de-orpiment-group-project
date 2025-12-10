import boto3
from botocore.stub import Stubber

client = boto3.client('s3')
stubber = Stubber(client)
list_buckets_response = {
    "Owner": {
        "DisplayName": "name",
        "ID": "EXAMPLE123"
    },
    "Buckets": [{
        "CreationDate": "2016-05-25T16:55:48.000Z",
        "Name": "foo"
    }]
}
expected_params = {}
stubber.add_response('list_buckets', list_buckets_response, expected_params)

with stubber:
    response = client.list_buckets()

assert response == list_buckets_response