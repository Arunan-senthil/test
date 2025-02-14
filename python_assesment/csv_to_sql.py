from extract_data_from_csv import CSVReader
from transform_data_in_python import DataCleaner
from upload_data_to_sql import MySQLTableCreator
import os
import glob
import pandas as pd
from dotenv import load_dotenv


def csv_to_sql(csv_file):
    try:

        # Read each CSV file into a DataFrame and store in a list
        csv_reader = CSVReader(csv_file)
        df = csv_reader.read_csv()

        cleaner = DataCleaner()

        # Clean the DataFrame step by step
        df = cleaner.clean_date_column(df, 'business_date')
        df = cleaner.unify_numeric_format(df)
        df = cleaner.remove_duplicates(df)
        df = cleaner.remove_extra_spaces(df)

        print(df)
        table_creator = MySQLTableCreator()

        sql_path = os.getenv('SQL_FILE_PATH')



        # Execute all SQL files in the current directory
        table_creator.execute_all_sql_files(directory=sql_path)

        # Upload all CSV files in the current directory to the respective tables

        table_name = os.path.basename(csv_file).replace(".csv", "")
        table_creator.upload_data_from_dataframe(table_name, df)

        # Close the MySQL connection
        table_creator.close_connection()

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    load_dotenv()
    csv_path = os.getenv('FILE_PATH')
    csv_files = glob.glob(os.path.join(csv_path, '*.csv'))
    for csv_file in csv_files:
        csv_to_sql(csv_file)



