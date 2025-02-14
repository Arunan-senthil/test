import os
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv


class MySQLTableCreator:
    def __init__(self):
        """
        Initializes the class, loads the environment variables, and sets up the MySQL connection.
        """
        load_dotenv()  # Load environment variables from .env file

        # Fetch MySQL credentials from environment variables
        self.host = os.getenv('MYSQL_HOST')
        self.user = os.getenv('MYSQL_USER')
        self.password = os.getenv('MYSQL_PASSWORD')
        self.database_name = os.getenv('MYSQL_DATABASE')

        self.connection = None
        self.cursor = None

        # Initialize MySQL connection (to check if database exists)
        self._connect_to_mysql()

    def _connect_to_mysql(self):
        """
        Connects to the MySQL server (not the database yet) using credentials from the .env file.
        """
        try:
            # Connect to MySQL server (without selecting a database)
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print("Successfully connected to MySQL server.")

            # Check if the database exists, and create it if it doesn't
            self._create_database_if_not_exists()

            # Connect to the specific database now that it's created (or verified)
            self.connection.database = self.database_name
            print(f"Connected to the database: {self.database_name}")

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your MySQL username or password.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist.")
            else:
                print(err)
            raise

    def _create_database_if_not_exists(self):
        """
        Creates the database if it doesn't exist.
        """
        try:
            # Check if the database exists
            self.cursor.execute(f"SHOW DATABASES LIKE '{self.database_name}'")
            result = self.cursor.fetchone()

            if not result:
                # Database does not exist, so create it
                print(f"Database '{self.database_name}' does not exist. Creating it...")
                self.cursor.execute(f"CREATE DATABASE {self.database_name}")
                print(f"Database '{self.database_name}' created successfully.")
            else:
                print(f"Database '{self.database_name}' already exists.")

        except mysql.connector.Error as err:
            print(f"Error checking or creating database: {err}")
            raise

    def execute_sql_file(self, file_path):
        """
        Executes SQL statements from a file.

        :param file_path: The path to the SQL file.
        """
        if not os.path.exists(file_path):
            print(f"File {file_path} not found.")
            return

        with open(file_path, 'r') as file:
            sql = file.read()

        try:
            # Execute the SQL commands
            self.cursor.execute(sql)
            self.connection.commit()
            print(f"SQL file '{file_path}' executed successfully.")
        except mysql.connector.Error as err:
            print(f"Error executing file '{file_path}': {err}")
            self.connection.rollback()

    def execute_all_sql_files(self, directory='.'):
        """
        Executes all SQL files in the given directory (default is the current directory).

        :param directory: Directory where SQL files are located.
        """
        for filename in os.listdir(directory):
            if filename.endswith(".sql"):
                file_path = os.path.join(directory, filename)
                self.execute_sql_file(file_path)

    def upload_data_from_dataframe(self, table_name, df):
        """
        Uploads data from a pandas DataFrame into a MySQL table.

        :param table_name: The name of the table where data should be uploaded.
        :param df: pandas DataFrame containing the data to be uploaded.
        """
        if df.empty:
            print(f"The DataFrame is empty. No data to upload to '{table_name}'.")
            return

        try:
            # Prepare the insert query based on the DataFrame columns
            columns = ', '.join(df.columns)
            values = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

            # Insert the data into the table
            for row in df.itertuples(index=False, name=None):
                self.cursor.execute(insert_query, row)

            # Commit the transaction
            self.connection.commit()
            print(f"Data from the DataFrame uploaded successfully into '{table_name}'.")

        except mysql.connector.Error as err:
            print(f"Error uploading data from DataFrame to '{table_name}': {err}")
            self.connection.rollback()

    def upload_all_csv_files(self, directory='.'):
        """
        Uploads data from all CSV files in the specified directory into the respective tables.

        :param directory: Directory where CSV files are located.
        """
        for filename in os.listdir(directory):
            if filename.endswith(".csv"):
                table_name = os.path.splitext(filename)[0]  # Assume table name is the CSV file name
                file_path = os.path.join(directory, filename)
                self.upload_data_from_csv(table_name, file_path)

    def close_connection(self):
        """
        Closes the MySQL connection and cursor.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("MySQL connection closed.")


# Example usage:

if __name__ == "__main__":
    # Initialize MySQLTableCreator
    table_creator = MySQLTableCreator()

    # Execute all SQL files in the current directory
    table_creator.execute_all_sql_files(directory=".")

    # Upload all CSV files in the current directory to the respective tables
    table_creator.upload_all_csv_files(directory=".")

    # Close the MySQL connection
    table_creator.close_connection()
