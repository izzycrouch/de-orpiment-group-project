from extract_layer.utils.connection import connect_to_db, close_db_connection
from unittest.mock import MagicMock, patch

class TestDBConnection:

    @patch('extract_layer.utils.connection.get_db_credentials')
    @patch('extract_layer.utils.connection.pg8000.native.Connection')
    def test_db_connection(self, mock_connect, mock_get_secrets):
        mock_get_secrets.return_value = {
            'user': 'test_user',
            'password': 'test_password',
            'host': 'test_host',
            'port': 1234,
            'database': 'test_db'
        }

        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        conn = connect_to_db()

        mock_get_secrets.assert_called_once()

        mock_connect.assert_called_once_with(
            user='test_user',
            password='test_password',
            host='test_host',
            port=1234,
            database='test_db'
        )

        mock_connect.assert_called_once()
        assert conn == mock_conn


    def test_close_connection(self):
        mock_conn = MagicMock()
        close_db_connection(mock_conn)

        mock_conn.close.assert_called_once()

