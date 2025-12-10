import pytest
import json
import boto3
from moto import mock_aws
from botocore.exceptions import ClientError
from src.utils.get_secret import get_secret


class TestGetSecretWithMoto:

    @pytest.fixture
    def aws_credentials(self, monkeypatch):

        monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
        monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
        monkeypatch.setenv('AWS_SECURITY_TOKEN', 'testing')
        monkeypatch.setenv('AWS_SESSION_TOKEN', 'testing')
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'eu-west-2')

    @pytest.fixture
    def secrets_manager_client(self):
        with mock_aws():
            yield boto3.client('secretsmanager', region_name='eu-west-2')

    @pytest.fixture
    def sample_secret(self, secrets_manager_client):

        secret_name = 'test-database-credentials'
        secret_value = {
            "database": "test",
            "host": "test-db.example.com",
            "port": 5432,
            "username": "testu",
            "password": "test"
        }

        secrets_manager_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(secret_value)
        )

        return secret_name, secret_value

    @mock_aws
    def test_get_secret_returns_dictionary(self, aws_credentials, sample_secret):

        secret_name, expected_output = sample_secret

        result = get_secret(secret_name, region_name='eu-west-2')

        assert isinstance(result, dict)


    def test_get_secret_returns_expected_keys(self, aws_credentials, sample_secret):

        secret_name, expected_output = sample_secret
        expected_keys = list(expected_output.keys())

        result = get_secret(secret_name, region_name='eu-west-2')
        list_keys = list(result.keys())

        assert list_keys == expected_keys


    def test_get_secret_returns_expected_values(self, aws_credentials, sample_secret):

        secret_name, expected_output = sample_secret

        result = get_secret(secret_name, region_name='eu-west-2')

        assert result['database'] == expected_output['database']
        assert result['host'] == expected_output['host']
        assert result['port'] == expected_output['port']
        assert result['username'] == expected_output['username']
        assert result['password'] == expected_output['password']

    def test_get_sectret_raises_error(self, aws_credentials, sample_secret):
        secret_name, expected_output = sample_secret

        with pytest.raises(ClientError):
            get_secret(secret_name, region_name='eu-west-1')