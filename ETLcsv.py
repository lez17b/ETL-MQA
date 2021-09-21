import pandas as pd
import mysql.connector
from mysql.connector import Error
# import vertica_db_client

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
        df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

    # NaN field handler for specific rows
    # Using PANDAS functions
    def nan_handler(self, field):
        self.df[field] = self.df[field].replace("NaN", " ")
        df = self.df
        df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

    # Zero value in field to null text value
    # Using PANDAS functions
    def zero_handler(self, field):
        self.df[field] = self.df[field].replace("null", 0)
        df = self.df
        df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

    # Zero value in field to empty field
    # Using PANDAS functions
    def zero_handler_null(self, field):
        self.df[field] = self.df[field].replace(" ", 0)
        df = self.df
        df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

############################################################
###                  Replacement handler                 ###
############################################################

    def replace_values(self, field, fr, to):
        df = self.df
        df[field] = df[field].replace(to, fr)
        df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

############################################################
###                  Date value handlers                 ###
############################################################

    # Date format handler type 1 = dd/mm/yyyy
    # Using PANDAS functions
    def date_to_dmy(self, field):
        self.df[field] = self.df[field].dt.strftime("%d/%m/%y")
        self.df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

    # Date format handler type 2 = mm/dd/yyyy
    # Using PANDAS functions
    def date_to_mdy(self, field):
        self.df[field] = self.df[field].dt.strftime("%m/%d/%y")
        self.df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

    # Date format handler type 3 = yyyy/mm/dd
    # Using PANDAS functions
    def date_to_ymd(self, field):
        self.df[field] = self.df[field].dt.strftime("%y/%m/%d")
        self.df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

    # Date format handler type 4 = yyyy/dd/mm
    # Using PANDAS functions
    def date_to_ydm(self, field):
        self.df[field] = self.df[field].dt.strftime("%y/%d/%m")
        self.df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

    # Advanced date format handler it requires to type the date format
    # Using PANDAS functions
    def date_adv_handler(self, format_base, field):
        # print("Please enter the date format you want in this format: %x/%y/%z example: %d/%m/%y")
        self.df[field] = pd.to_datetime(self.df[field], format=format_base)
        self.df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

    # Integer to date handler
    # Using PANDAS functions
    def int_to_date(self, field):
        self.df.to_datetime(str(field), format='%Y-%m-%d')
        self.df.to_csv("new_" + field + ".csv")
        stream = "new_" + filed + ".csv"
        return stream

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
        df.to_csv("groupBy_" + field1 + "_" + field2 + "_" + df + ".csv")
        stream = "groupBy_" + field1 + "_" + field2 + "_" + df + ".csv"
        return stream

    # Transpose method
    # Using PANDAS functions
    def transpose_df(self):
        df = self.df.T
        df.to_csv("transposed" + df + ".csv")
        stream = "transposed" + df + ".csv"
        return stream

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
        df.to_csv("operation_" + operation + ".csv")
        stream = "operation_" + operation + ".csv"
        return stream

    # Truncate the data frame by reducing rows from before to after
    # Using PANDAS functions
    def truncate_df(self, before, after):
        df = self.df
        df.truncate(before=before, after=after)
        df.to_csv("new_truncated.csv")
        stream = "new_truncated.csv"
        return stream

    # Truncate the data frame by reducing columns from before to after
    # Using PANDAS functions
    def truncate_df_col(self, before, after, axis):
        df = self.df
        df.truncate(before=before, after=after, axis=axis)
        df.to_csv("new_truncated.csv")
        stream = "new_truncated.csv"
        return stream

    # Truncate the data frame by date from before to after
    # Using PANDAS functions
    def truncate_df_time(self, before, after):
        df = self.df
        b = pd.Timestamp(before)
        a = pd.Timestamp(after)
        df.truncate(before=b, after=a).tail()
        df.to_csv("new_truncated.csv")
        stream = "new_truncated.csv"
        return stream

    # Unstack function based on a dataframe
    # Using PANDAS functions
    def unstack_df(self, level):
        df = self.df
        df.unstack(level=level)
        df.to_csv("new_unstacked.csv")
        stream = "new_unstacked.csv"
        return stream


# MySQL Database manager class
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


# Vertica SQL Manager class
class DatabaseManagerVertica:

    # Constructor
    def __init__(self, database, user, password):
        self.database = database
        self.user = user
        self.password = password
        try:
            self.db = vertica_db_client.connect(database=self.database,
                                                user=self.user,
                                                password=self.password)
            if self.db.is_connected():
                db_info = self.db.get_server_info()
                print("Connected to Vertica Server version ", db_info)
            self.cursor = self.db.cursor()

        except Error as e:
            print("Error while connecting to Vertica", e)
        finally:
            if self.db.is_connected():
                self.db.close()
                print("Vertica connection is closed")

    # Execute query function
    # using Vertica methods
    def execute_query(self, query):
        result = self.cursor.execute(query)
        return result

    # Fetch rows function
    # using Vertica methods
    def fetch_rows(self):
        rows = self.cursor.fetchall()
        return rows

    # Print table function
    # using Vertica methods
    def print_data(self):
        rows = self.cursor.fetchall()
        for i, row in enumerate(rows):
            print("Row", i, "Data = ", row)


