import pandas as pd
import mysql.connector
from mysql.connector import Error

#################################################################
##             Luciano Zavala´s ETL personal library           ##
##          Built on top of NumPy and Pandas frameworks.       ##
## The library is designed to be used as a scientific library. ##
#################################################################


# class for CSV extraction
class ExtractTransformLoad:

    # Constructor
    def __init__(self, data):
        self.df = pd.read_csv(data, encoding='unicode_escape',  error_bad_lines=False)

############################################################
###           Empty, null, NaN value handlers            ###
############################################################

    # Null field handler for specific rows
    # Using PANDAS functions
    def null_handler(self, field):
        self.df[field] = self.df[field].replace(0, " ")
        df = self.df
        return self.df.to_csv("new_" + field + ".csv")

    # NaN field handler for specific rows
    # Using PANDAS functions
    def nan_handler(self, field):
        self.df[field] = self.df[field].replace("NaN", " ")
        df = self.df
        return df.to_csv("new_" + field + ".csv")

    # Zero value in field to null text value
    # Using PANDAS functions
    def zero_handler(self, field):
        self.df[field] = self.df[field].replace("null", 0)
        df = self.df
        return df.to_csv("new_" + field + ".csv")

    # Zero value in field to empty field
    # Using PANDAS functions
    def zero_handler_null(self, field):
        self.df[field] = self.df[field].replace(" ", 0)
        df = self.df
        return df.to_csv("new_" + field + ".csv")

############################################################
###                  Replacement handler                 ###
############################################################

    def replace_values(self, field, fr, to):
        df = self.df
        df[field] = df[field].replace(to, fr)
        return df.to_csv("new_" + field + ".csv")

############################################################
###                  Date value handlers                 ###
############################################################

    # Date format handler type 1 = dd/mm/yyyy
    # Using PANDAS functions
    def date_to_dmy(self, field):
        self.df[field] = self.df[field].dt.strftime("%d/%m/%y")
        return self.df.to_csv("new_" + field)

    # Date format handler type 2 = mm/dd/yyyy
    # Using PANDAS functions
    def date_to_mdy(self, field):
        self.df[field] = self.df[field].dt.strftime("%m/%d/%y")
        return self.df.to_csv("new_" + field)

    # Date format handler type 3 = yyyy/mm/dd
    # Using PANDAS functions
    def date_to_ymd(self, field):
        self.df[field] = self.df[field].dt.strftime("%y/%m/%d")
        return self.df.to_csv("new_" + field)

    # Date format handler type 4 = yyyy/dd/mm
    # Using PANDAS functions
    def date_to_ydm(self, field):
        self.df[field] = self.df[field].dt.strftime("%y/%d/%m")
        return self.df.to_csv("new_" + field)

    # Advanced date format handler it requires to type the date format
    # Using PANDAS functions
    def date_adv_handler(self, format_base, field):
        # print("Please enter the date format you want in this format: %x/%y/%z example: %d/%m/%y")
        self.df[field] = pd.to_datetime(self.df[field], format=format_base)
        return self.df.to_csv("new_" + field + ".csv")

    # Integer to date handler
    # Using PANDAS functions
    def int_to_date(self, field):
        self.df.to_datetime(str(field), format='%Y-%m-%d')
        return self.df.to_csv("new_" + field + ".csv")

############################################################
###                  Data visualizers                    ###
############################################################

    # Print the data frame
    # Using PANDAS functions
    def print_df(self):
        print(self.df)

    # data frame head
    # Using PANDAS functions
    def df_head(self):
        print(self.df.head())

    # data frame tail
    # Using PANDAS functions
    def df_tail(self):
        print(self.df.tail())

############################################################
###                Transformation methods                ###
############################################################

    # Group bu method in order to filter fields
    # Using PANDAS functions
    def group_by_df(self, field1, field2, operation):
        df = self.df
        df.groupby(field1)[field2].transform(operation)
        return df.to_csv("groupBy_" + field1 + "_" + field2 + "_" + df + ".csv")

    # Transpose method
    # Using PANDAS functions
    def transpose_df(self):
        df = self.df.T
        return df.to_csv("transposed" + df + ".csv")

    # Matrix operations using the pandas operation tool set
    # Using PANDAS functions
    def linear_algebra(self, operation, amount):
        df = self.df
        if operation == 'sum':
            df = df.sum(amount)
        elif operation == 'div':
            df = df.div(amount)
        elif operation == 'reverse-div':
            df = df.rdiv(amount)
        elif operation == 'multiply':
            df = df * amount
        return df.to_csv("operation_" + operation + ".csv")

    # Truncate the data frame by reducing rows from before to after
    # Using PANDAS functions
    def truncate_df(self, before, after):
        df = self.df
        df.truncate(before=before, after=after)
        return df.to_csv("new_truncated.csv")

    # Truncate the data frame by reducing columns from before to after
    # Using PANDAS functions
    def truncate_df_col(self, before, after, axis):
        df = self.df
        df.truncate(before=before, after=after, axis=axis)
        return df.to_csv("new_truncated.csv")

    # Truncate the data frame by date from before to after
    # Using PANDAS functions
    def truncate_df_time(self, before, after):
        df = self.df
        b = pd.Timestamp(before)
        a = pd.Timestamp(after)
        df.truncate(before=b, after=a).tail()
        return df.to_csv("new_truncated.csv")

    # Unstack function based on a dataframe
    # Using PANDAS functions
    def unstack_df(self, level):
        df = self.df
        df.unstack(level=level)
        return df.to_csv("new_unstacked.csv")


class DatabaseManagerMySQL:

    # Constructor
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        try:
            self.connection = mysql.connector.connect(host=self.host,
                                                      database=self.database,
                                                      user=self.user,
                                                      password=self.password)
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                print("Connected to MySQL Server version ", db_info)

        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if self.connection.is_connected():
                self.connection.close()
                print("MySQL connection is closed")

    # Execute query function
    # using MySQL methods
    def execute_query(self, query):
        cursor = self.connection.cursor()
        result = cursor.execute(query)
        return result

    # Close connection function
    # Using MySQL methods
    def close_connection(self):
        self.connection.close()







