import yaml
# import sqlalchemy as db
from sqlalchemy import create_engine, inspect
# from sqlalchemy_utils import database_exists, create_database
# from data_cleaning import DataCleaning
class DatabaseConnector():
    def __init__(self):
        pass
    def read_db_creds(self):
        with open('db_creds.yaml','r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds
    def init_db_engine(self):
        db_conn_details = self.read_db_creds()
        self.user = db_conn_details['RDS_USER']
        self.password = db_conn_details['RDS_PASSWORD']
        self.host = db_conn_details['RDS_HOST']
        self.database = db_conn_details['RDS_DATABASE']
        self.port  = db_conn_details['RDS_PORT']
        self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
        return self.engine

    def list_db_tables(self):
        self.engine = self.init_db_engine()
        self.inspector = inspect(self.engine)
        self.tables = self.inspector.get_table_names()
        return self.tables

    def upload_to_db(self,df,table_name):
        self.host = 'localhost'
        self.user = 'postgres'
        self.password = 1234
        self.port = 5432
        self.database = 'sales_data'
        self.selected_engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
        # self.selected_engine = create_engine('postgresql://postgres:1234@localhost/sales_data')
        # engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')

        self.selected_engine.connect()
        df.to_sql(table_name, self.selected_engine)

# mydb = DatabaseConnector()
# mydb.read_db_creds()
# mydb.init_db_engine()
# print(mydb.list_db_tables())

# datacleaner = DataCleaning()
# cleaned_df = datacleaner.clean_user_data()
# mydb.upload_to_db(cleaned_df,'dim_users')