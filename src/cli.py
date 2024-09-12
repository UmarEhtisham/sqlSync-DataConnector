import click

Yellow = '\033[93m'
RESET = '\033[0m'

class CLI:

        
    def prompt_connection_input(self):

        host_name = click.prompt(f"{Yellow}MySQL Host{RESET}", default='localhost')
        port = click.prompt(f"{Yellow}MySQL Port{RESET}", default=3306, type=int)
        user_name = click.prompt(f"{Yellow}MySQL User{RESET}", default='root')
        user_password = click.prompt(f"{Yellow}MySQL Password{RESET}", default='jndvOs@24', hide_input=True)
        
        return host_name, port, user_name, user_password
    
    def choice_input(self):

        action = click.prompt(f"{Yellow}File(s) Import/Export{RESET}").lower()

        return action
    
    # Import related inputs starts here
    
    def database_name_input(self):

        database_name = click.prompt(f"{Yellow}Create Database Name{RESET}", default='', type=str)

        return database_name
    
    def directory_path_import(self):

        directory_path = click.prompt(f"{Yellow}Import Directory Path{RESET}", default='.')

        return directory_path
    
    def import_related_options_input(self):

        files_selected = click.prompt(f"{Yellow}Enter the keys of the files you want to select (comma-separated, e.g., 1,2,3...) or type 'all' to select all files{RESET}", default='all')

        if files_selected == 'all':

            return files_selected
        else:
            selected_numbers = [int(x)-1 for x in files_selected.split(',')]

            return selected_numbers

# export related inputs starts here
    def detabase_selection(self):

        database_selected = click.prompt(f"{Yellow}Enter the key of the database you want to select (comma-separated, e.g., 1,2,3...){RESET}")

        selected_numbers = [int(x) for x in database_selected.split(',')]

        return selected_numbers
    
    def tables_selection(self):

        tables_selected = click.prompt(f"{Yellow}Enter the keys of the files you want to select (comma-separated, e.g., 1,2,3...) or type 'all' to select all files{RESET}", default='all')

        if tables_selected == 'all':

            return tables_selected
        else:
            selected_numbers = [int(x) for x in tables_selected.split(',')]

            return selected_numbers
    
    def format_selection(self):
        
        format_dict = {
            1: 'CSV',
            2: 'Excel',    
            3: 'JSON'
        }
        for key, value in format_dict.items():
            click.echo(f"{key}. {value}")

        format_selected = click.prompt(f"{Yellow}Enter the key of the format you want to select{RESET}")

        if format_selected == '1':
            return 'CSV'
        elif format_selected == '2':
            return 'Excel'
        else:
            return 'JSON'


    def directory_path_export(self):

        directory_path = click.prompt(f"{Yellow}Export Directory Path{RESET}", default='.')

        return directory_path
    
    def excel_export_related_options(self):

        format_dict = {
            1: f"{Yellow}Single file (database name) with multiple sheets (tables){RESET}",
            2: f"{Yellow}Tables as separate files{RESET}"
        }
        for key, value in format_dict.items():
            click.echo(f"{key}. {value}")

        format_selected = click.prompt(f"{Yellow}Enter the key of the format you want to select{RESET}")

        if format_selected == '1':
            return 'single_file'
        elif format_selected == '2':
            return 'separate_files'
     
