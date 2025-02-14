import pandas as pd

class DataCleaner:
    def __init__(self):
        """
        Initializes the DataCleaner class.
        """
        pass

    def clean_date_column(self, df, date_column):
        """
        Converts date columns to a uniform date format (YYYY-MM-DD),
        while handling multiple formats and separators.

        :param df: pandas DataFrame that needs cleaning.
        :param date_column: Column name to be converted to date format.
        :return: Cleaned pandas DataFrame.
        """
        # Replace multiple separators with a standard separator (e.g., '-')
        df[date_column] = df[date_column].astype(str).str.replace(r'[./\s]', '-', regex=True)

        # Convert the date column to datetime, handling various formats
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce', dayfirst=False).dt.strftime('%Y-%m-%d')

        print(f"Date column '{date_column}' cleaned.")
        return df

    def unify_numeric_format(self, df):
        """
        Unifies the numeric columns to have consistent formatting (integer or float).

        :param df: pandas DataFrame that needs cleaning.
        :return: Cleaned pandas DataFrame.
        """
        for column in df.select_dtypes(include=['float64', 'int64']).columns:
            # Convert all numeric columns to float for consistency
            df[column] = df[column].apply(pd.to_numeric, errors='coerce')

        print("Numeric columns unified.")
        return df

    def remove_duplicates(self, df, subset=None):
        """
        Removes duplicate rows from the DataFrame.

        :param df: pandas DataFrame that needs cleaning.
        :param subset: List of columns to check for duplicates (optional).
        :return: Cleaned pandas DataFrame.
        """
        initial_row_count = len(df)
        df.drop_duplicates(subset=subset, keep='first', inplace=True)
        final_row_count = len(df)
        print(f"Removed {initial_row_count - final_row_count} duplicate rows.")
        return df

    def remove_extra_spaces(self, df):
        """
        Removes extra spaces from string columns (leading, trailing, and redundant internal spaces).

        :param df: pandas DataFrame that needs cleaning.
        :return: Cleaned pandas DataFrame.
        """
        # Iterate over all string columns
        for column in df.select_dtypes(include=['object']).columns:
            df[column] = df[column].str.strip().str.replace(r'\s+', ' ', regex=True)

        print("Extra spaces removed from string columns.")
        return df
