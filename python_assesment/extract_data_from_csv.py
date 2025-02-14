import pandas as pd


class CSVReader:
    def __init__(self, file_path):
        """
        Initializes the CSVReader class with the given file path.

        :param file_path: Path to the CSV file.
        """
        self.file_path = file_path

    def read_csv(self):
        """
        Reads the CSV file from the given path and returns a pandas DataFrame.

        :return: DataFrame containing the data from the CSV file.
        """
        try:
            # Read the CSV file using pandas
            data_frame = pd.read_csv(self.file_path)
            return data_frame
        except FileNotFoundError:
            print(f"Error: The file at {self.file_path} was not found.")
        except pd.errors.EmptyDataError:
            print("Error: The CSV file is empty.")
        except pd.errors.ParserError:
            print("Error: There was an issue parsing the CSV file.")
        except Exception as e:
            print(f"An error occurred: {e}")