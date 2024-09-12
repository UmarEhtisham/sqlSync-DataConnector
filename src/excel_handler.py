import pandas as pd
import logging
import pathlib
from pathlib import Path
from datetime import datetime

# logging to print, warning and error   
logging.basicConfig(level=logging.INFO, format = '%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExcelHandler:

    def __init__(self, engine, database_name):
       
       self.database_name = database_name
       
       self.engine = engine

    def import_excel_files(self, excel_files):

        self.excel_files = excel_files

        for excel_file in self.excel_files:
            directory = excel_file.parent
            file_name = excel_file.stem

            excel_data = pd.read_excel(excel_file, sheet_name=None)

            for sheet_name, df in excel_data.items():

                if df.isnull().sum().sum() > 0: 
                    logger.warning(f"NaN values found in '{file_name}'")
                    df = df.where(pd.notnull(df), None)
                    logger.info(f"NaN values have been fixed for '{file_name}'")

                df.columns = df.columns.str.replace(r'[ .-]', '_', regex=True).str.lower()  

                logger.info(f"Column name discrepencies have been fixed for '{sheet_name}' ")

                df.to_sql(name = sheet_name,  con = self.engine, schema = self.database_name, if_exists = 'replace', index=False)

            logger.info(f"'{file_name}' imported successfully, from '{directory}'\n")

    def export_excel_files(self, export_path, excel_tables, file_option):

        export_path = Path(export_path)

        timestamp = datetime.now().strftime("%Y%m%d")

        directory = export_path / f"Dump_{timestamp}"

        directory.mkdir(parents=True, exist_ok=True) 

        if file_option == 'single_file':
                
            excel_file_path = directory / f"{self.database_name}.xlsx"

            with pd.ExcelWriter(excel_file_path) as writer:

                for table in excel_tables:

                    query = f"SELECT * FROM {self.database_name}.{table}"

                    df = pd.read_sql(query, con = self.engine)
                    df.to_excel(writer, sheet_name=table, index = False)

                    logger.info(f"Table '{table}' successfully added to {self.database_name} and exported to {excel_file_path}")


        elif file_option == 'separate_files':

            for table in excel_tables:
                
                excel_file_path = directory / f"{table}.xlsx"

                query = f"SELECT * FROM {self.database_name}.{table}"

                df = pd.read_sql(query, con = self.engine)
                df.to_excel(excel_file_path, index = False)

                logger.info(f"Table '{table}' exported successfully to {excel_file_path}")

   


if __name__ == '__main__':

    pass