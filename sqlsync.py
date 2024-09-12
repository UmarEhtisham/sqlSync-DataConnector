# import numexpr as ne
# ne.set_num_threads(8)
# print("Number of threads set to:", ne.get_num_threads())

from src.cli import CLI
from src.db_connection import DBConnection
from src.csv_handler import CSVHandler
from src.excel_handler import ExcelHandler
from src.json_handler import JSONHandler

import logging
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
from pymysql.err import MySQLError, OperationalError, ProgrammingError
import pathlib
from pathlib import Path

import logging

logging.basicConfig(level=logging.INFO, format = '%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLSync:

    def __init__(self, host_name, port, user_name, user_password):

        self.host_name = host_name
        self.port = port
        self.user_name = user_name
        self.user_password = user_password
        self.db_connection_instance = DBConnection(self.host_name, self.port, self.user_name, self.user_password)
        self.connection = self.db_connection_instance.create_connection()
 
    def handle_impex(self, impex_choice):

        if impex_choice == 'import':
            self.database_name = cli.database_name_input()
            self.database_creation()
            self.import_path = cli.directory_path_import()
            self.directory_access()
            self.selection = cli.import_related_options_input()
            self.files_to_be_imported()
            
        elif impex_choice == 'export':

            self.access_databases()
            self.selected_database = cli.detabase_selection()
            self.databases_to_be_exported()
            
        else:
            logger.error('Invalid choice')
            exit(1)

# import related methods starts from here

    def database_creation(self):

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            logger.info("Database created successfully")

        except MySQLError as e:
            logger.error(f'Error creating database: {e}')

        except Exception as e:
            logger.error(f'Unexpected error: {e}')

    def directory_access(self):

        self.content = None

        try:    
            path = Path(self.import_path)
            if not path.exists():
                logger.error(f"Directory path {path} is not accurate/exists")
            if path.is_dir():
                self.content = {number: file for number, file in enumerate(path.iterdir()) if file.is_file()}
                for index, file in self.content.items():
                    print(f'{index+1}. {file.name}')
            return self.content
            
        except Exception as e:  
            logger.error(f'Unexpected error in retreiving files: {e}')
    
    def files_to_be_imported(self):

        self.segregation_list = []

        try:
            if self.selection == 'all':
                for _, file in self.content.items():
                    self.segregation_list.append(file)
            
            elif isinstance(self.selection, list):
            
                for index, file in self.content.items():
                    if index in self.selection:
                        self.segregation_list.append(file)

        except Exception as e:  
            logger.error(f'Unexpected error in retreiving files: {e}')

        self.files_to_be_segregated()

    def files_to_be_segregated(self):

        self.csv_list = []
        self.excel_list = []
        self.json_list = []

        for file_path in self.segregation_list: 
                extension = file_path.name.split('.')[-1]
                if extension == 'csv':
                    self.csv_list.append(file_path)
                elif extension == 'xlsx' or extension == 'xls':
                    self.excel_list.append(file_path)
                elif extension == 'json':
                    self.json_list.append(file_path)
                    
        self.file_manager_imports()

    def file_manager_imports(self):

        self.engine = self.db_connection_instance.create_engine(self.database_name)
        self.csv_handler_instance = CSVHandler(self.engine, self.database_name)
        self.excel_handler_instance = ExcelHandler(self.engine, self.database_name)
        self.json_handler_instance = JSONHandler(self.engine, self.database_name)

        self.csv_handler_instance.import_csv_files(self.csv_list)
        self.excel_handler_instance.import_excel_files(self.excel_list)
        self.json_handler_instance.import_json_files(self.json_list)

# export related methods starts from here

    def access_databases(self):

        self.databases_dict = {}

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW DATABASES")
                databases = cursor.fetchall()
                self.databases_dict = {number+1: database[0] for number, database in enumerate(databases)}

                logger.info("List of databases:")

                for index, database in self.databases_dict.items():
                    print(f'{index}. {database}')
                    
        except MySQLError as e:
            logger.error(f'Error creating database: {e}')

        except Exception as e:
            logger.error(f'Unexpected error: {e}')

    def databases_to_be_exported(self):

        self.segregated_databases_list = []


        for index, database in self.databases_dict.items():
           
            if index in self.selected_database:
                self.segregated_databases_list.append(database)

        print("self.segregated_databases_list", self.segregated_databases_list)

        self.tables_to_be_shown()

    def tables_to_be_shown(self):

        self.tables_dict = {}

        for database in self.segregated_databases_list:
            with self.connection.cursor() as cursor:
                cursor.execute(f"SHOW TABLES IN {database}")
                tables = cursor.fetchall()

                self.tables_dict.clear()

                self.tables_dict[database] = {number+1: table[0] for number, table in enumerate(tables)}

            for database, tables in self.tables_dict.items():
                print()
                logger.info(f"Database : '{database}'")
                logger.info(f"List of Tables in Database")
                for key, value in tables.items():
                    print(f"{key}: {value}")
   
            self.tables_to_be_exported()

    def tables_to_be_exported(self):

        self.tables_selected = cli.tables_selection()
        self.format = cli.format_selection()
        

        if self.tables_selected == 'all' and self.format == 'CSV':
            database_name = [database for database, _ in self.tables_dict.items()]
            csv_tables = [tables for sub_dict in self.tables_dict.values() for tables in sub_dict.values()]
            export_path = cli.directory_path_export()
            self.file_manager_csv_exports(database_name, export_path, csv_tables)
          
            
        elif isinstance(self.tables_selected, list) and self.format == 'CSV':
            database_name = [database for database, _ in self.tables_dict.items()]
            csv_tables = [tables for _, sub_dict in self.tables_dict.items() for number, tables in sub_dict.items() if number in self.tables_selected] 
            export_path = cli.directory_path_export()
            self.file_manager_csv_exports(database_name, export_path, csv_tables)


        elif self.tables_selected == 'all' and self.format == 'Excel':
            database_name = [database for database, _ in self.tables_dict.items()]
            excel_tables = [tables for sub_dict in self.tables_dict.values() for tables in sub_dict.values()]
            export_path = cli.directory_path_export()
            file_option = cli.excel_export_related_options()
            self.file_manager_excel_exports(database_name, export_path, excel_tables, file_option)

            
        elif isinstance(self.tables_selected, list) and self.format == 'Excel':
            database_name = [database for database, _ in self.tables_dict.items()]
            excel_tables = [tables for _, sub_dict in self.tables_dict.items() for number, tables in sub_dict.items() if number in self.tables_selected] 
            export_path = cli.directory_path_export()
            file_option = cli.excel_export_related_options()
            self.file_manager_excel_exports(database_name, export_path, excel_tables, file_option)

        elif self.tables_selected == 'all' and self.format == 'JSON':
            database_name = [database for database, _ in self.tables_dict.items()]
            json_tables = [tables for sub_dict in self.tables_dict.values() for tables in sub_dict.values()]
            export_path = cli.directory_path_export()
            self.file_manager_json_exports(database_name, export_path, json_tables)

            
        elif isinstance(self.tables_selected, list) and self.format == 'JSON':
            database_name = [database for database, _ in self.tables_dict.items()]
            json_tables = [tables for _, sub_dict in self.tables_dict.items() for number, tables in sub_dict.items() if number in self.tables_selected] 
            export_path = cli.directory_path_export()
            self.file_manager_json_exports(database_name, export_path, json_tables)

    def file_manager_csv_exports(self, database_name, export_path, csv_tables):

        self.database_name = database_name[0]

        self.engine = self.db_connection_instance.create_engine(self.database_name)

        self.csv_handler_instance = CSVHandler(self.engine, self.database_name)
        self.csv_handler_instance.export_csv_files(export_path, csv_tables)

    def file_manager_excel_exports(self, database_name, export_path, excel_tables, file_option):

        self.database_name = database_name[0]

        self.engine = self.db_connection_instance.create_engine(self.database_name)

        self.excel_handler_instance = ExcelHandler(self.engine, self.database_name)
        self.excel_handler_instance.export_excel_files(export_path, excel_tables, file_option)


    def file_manager_json_exports(self, database_name, export_path, json_tables):

        self.database_name = database_name[0]

        self.engine = self.db_connection_instance.create_engine(self.database_name)
        
        self.json_handler_instance = JSONHandler(self.engine, self.database_name)
        self.json_handler_instance.export_json_files(export_path, json_tables)


    def run(self):
        
        impex_choice = cli.choice_input()
        self.handle_impex(impex_choice)
        self.db_connection_instance.close_connection()
      
if __name__ == "__main__":

    cli = CLI()
    sqlsync = SQLSync(*cli.prompt_connection_input())

    sqlsync.run()