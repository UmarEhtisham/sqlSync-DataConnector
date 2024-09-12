import pymysql
from pymysql.err import MySQLError, OperationalError, ProgrammingError
import logging
import sqlalchemy
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# logging to print, warning and error   
logging.basicConfig(level=logging.INFO, format = '%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DBConnection:
     
    def __init__(self, host_name, port, user_name, user_password):
         
        self.host_name = host_name
        self.port = port
        self.user_name = user_name
        self.user_password = user_password
        self.connection = None

    def create_connection(self):
        
        try:
            connection = pymysql.connect (
                host = self.host_name,
                port = self.port,
                user = self.user_name,
                password = self.user_password
            )

            logger.info("Connection created successfully")
            self.connection = connection
            return self.connection

        except MySQLError as e:
            logger.error(f'Error connecting to MySQL server: {e}')

        except Exception as e:
            logger.error(f'Unexpected error: {e}')
            
        return None

    def create_engine(self, database_name):

        self.database_name = database_name

        password = quote_plus(self.user_password)
        
        self.engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.user_name}:{password}@{self.host_name}:{self.port}/{self.database_name}")

        return self.engine
     
    def close_connection(self):
    
        if self.connection:
            self.connection.close()
            logger.info("Connection closed successfully")
