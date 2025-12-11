import pytest
import json
import boto3
from moto import mock_aws
from botocore.exceptions import ClientError
from extract_layer.utils.db_credentials import get_db_credentials


class TestGetDBCredentials:

    @pytest.fixture
    def mock_secrets_manager(self):
        with mock_aws():
            yield boto3.client('secretsmanager', region_name='eu-west-2')


    @pytest.fixture
    def sample_secret(self, mock_secrets_manager):
        secret_name = 'test-database-credentials'
        secret_value = {
            "database": "test_db",
            "host": "test-db.example.com",
            "port": 5432,
            "username": "test_user",
            "password": "test_pass"
        }

        mock_secrets_manager.create_secret(
            Name=secret_name,
            SecretString=json.dumps(secret_value)
        )

        return secret_name, secret_value

    @mock_aws
    def test_get_db_credentials_returns_dictionary(self, sample_secret):
        secret_name, _ = sample_secret

        result = get_db_credentials(secret_name, region_name='eu-west-2')

        assert isinstance(result, dict)


    def test_get_db_credentials_returns_expected_keys(self, sample_secret):
        secret_name, expected_output = sample_secret
        expected_keys = list(expected_output.keys())

        result = get_db_credentials(secret_name, region_name='eu-west-2')
        list_keys = list(result.keys())

        assert list_keys == expected_keys


    def test_get_db_credentials_returns_expected_values(self, sample_secret):
        secret_name, expected_output = sample_secret

        result = get_db_credentials(secret_name, region_name='eu-west-2')

        assert result['database'] == expected_output['database']
        assert result['host'] == expected_output['host']
        assert result['port'] == expected_output['port']
        assert result['username'] == expected_output['username']
        assert result['password'] == expected_output['password']


    def test_get_db_credentials_raises_error_with_incorrect_region_name(self, sample_secret):
        secret_name, _ = sample_secret

        with pytest.raises(ClientError):
            get_db_credentials(secret_name, region_name='eu-west-1')


    def test_get_db_credentials_raises_error_with_incorrect_secret_name(self, sample_secret):
        with pytest.raises(ClientError):
            get_db_credentials(secret_name = 'test_secret', region_name='eu-west-2')
