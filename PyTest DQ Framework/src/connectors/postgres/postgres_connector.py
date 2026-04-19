
import psycopg2

class PostgresConnectorContextManager:
    def __init__(self, db_host: str, db_name: str, db_user: str = None, db_password: str = None, db_port: int = 5432):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port
        self.connection = None

    def __enter__(self):
        self.connection = psycopg2.connect(
            host=self.db_host,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password,
            port=self.db_port
        )
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.connection:
            self.connection.close()

    def get_data_sql(self, sql):
        import pandas as pd
        return pd.read_sql(sql, self.connection)


