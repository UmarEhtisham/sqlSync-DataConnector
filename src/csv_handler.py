import pandas as pd
import logging
import pathlib
from pathlib import Path
from datetime import datetime

# logging to print, warning and error   
logging.basicConfig(level=logging.INFO, format = '%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVHandler:

    def __init__(self, engine, database_name):
       
       self.database_name = database_name
       
       self.engine = engine
    
    def import_csv_files(self, csv_files):

        self.csv_files = csv_files

        for csv_file in self.csv_files:
            directory = csv_file.parent
            file_name = csv_file.stem

            df = pd.read_csv(csv_file, encoding='utf-8')

            if df.isnull().sum().sum() > 0: 
                logger.warning(f"NaN values found in '{file_name}'")
                df = df.where(pd.notnull(df), None)
                logger.info(f"NaN values have been fixed for '{file_name}'")

            df.columns = df.columns.str.replace(r'[ .-]', '_', regex=True).str.lower() 

            logger.info(f"Column name discrepencies have been fixed for '{file_name}' ")
                
            df.to_sql(name = file_name,  con = self.engine, schema = self.database_name, if_exists = 'replace', index=False)
           
            logger.info(f"'{file_name}' imported successfully, from '{directory}'\n")

    def export_csv_files(self, export_path, csv_tables):

        export_path = Path(export_path)

        timestamp = datetime.now().strftime("%Y%m%d")

        directory = export_path / f"Dump_{timestamp}"

        directory.mkdir(parents=True, exist_ok=True) 

        for table in csv_tables:
            
            csv_file_path = directory / f"{table}.csv"

            query = f"SELECT * FROM {self.database_name}.{table}"

            df = pd.read_sql(query, con = self.engine)
            df.to_csv(csv_file_path, index = False)

            logger.info(f"Table '{table}' exported successfully to {csv_file_path}")


if __name__ == '__main__':

    pass



     
        
       