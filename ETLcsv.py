import numpy as np
import pandas as pd
# Luciano Zavala´s  personal library
# Built on top of NumPy and Pandas frameworks


# class for CSV extraction
class ExtractTransformLoad:

    # Constructor
    def __init__(self, data):
        self.df = pd.read_csv(data, encoding='unicode_escape',  error_bad_lines=False)

############################################################
###           Empty, null, NaN value handlers             ##
############################################################

    # Null field handler for specific rows
    def null_handler(self, field):
        self.df[field] = self.df[field].replace(0, " ")
        df = self.df
        return self.df.to_csv("new_" + field + ".csv")

    # NaN field handler for specific rows
    def nan_handler(self, field):
        self.df[field] = self.df[field].replace("NaN", " ")
        df = self.df
        return df.to_csv("new_" + field + ".csv")

    # Zero value in field to null text value
    def zero_handler(self, field):
        self.df[field] = self.df[field].replace("null", 0)
        df = self.df
        return df.to_csv("new_" + field + ".csv")

    # Zero value in field to empty field
    def zero_handler_null(self, field):
        self.df[field] = self.df[field].replace(" ", 0)
        df = self.df
        return df.to_csv("new_" + field + ".csv")

############################################################
###                  Date value handlers                  ##
############################################################

    # Date format handler type 1 = dd/mm/yyyy
    def date_to_dmy(self, field):
        self.df[field] = self.df[field].dt.strftime("%d/%m/%y")
        return self.df.to_csv("new_" + field)

    # Date format handler type 2 = mm/dd/yyyy
    def date_to_mdy(self, field):
        self.df[field] = self.df[field].dt.strftime("%m/%d/%y")
        return self.df.to_csv("new_" + field)

    # Date format handler type 3 = yyyy/mm/dd
    def date_to_ymd(self, field):
        self.df[field] = self.df[field].dt.strftime("%y/%m/%d")
        return self.df.to_csv("new_" + field)

    # Date format handler type 4 = yyyy/dd/mm
    def date_to_ydm(self, field):
        self.df[field] = self.df[field].dt.strftime("%y/%d/%m")
        return self.df.to_csv("new_" + field)

    # Advanced date format handler it requires to type the date format
    def date_adv_handler(self, format_base, field):
        # print("Please enter the date format you want in this format: %x/%y/%z example: %d/%m/%y")
        self.df[field] = pd.to_datetime(self.df[field], format=format_base)
        return self.df.to_csv("new_" + field)

    # Integer to date handler
    def int_to_date(self, field):
        self.df.to_datetime(str(field), format='%Y-%m-%d')
        return self.df.to_csv("new_" + field)

############################################################
###                  Data visualizers                     ##
############################################################

    # Print the data frame
    def print_df(self):
        print(self.df)

    # data frame head
    def df_head(self):
        print(self.df.head())

    # data frame tail
    def df_tail(self):
        print(self.df.tail())

############################################################
###                Transformation methods                ###
############################################################

    # Group bu method in order to filter filds
    def group_by_df(self, field1, field2, operation):
        df = self.df
        df.groupby(field1)[field2].transform(operation)
        return df.to_csv("groupBy_" + field1 + "_" + field2 + "_" + df + ".csv")

    # Transpose method
    def transpose_df(self):
        df = self.df.T
        return df.to_csv("transposed" + df + ".csv")

    # Matrix operations using the pandas operation tool set
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





