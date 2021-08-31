import numpy as np
import pandas as pd
# Luciano Zavala´s  personal library
# Built on top of NumPy and Pandas frameworks


# class for CSV extraction
class ExtractTransformLoad:

    # Constructor
    def __init__(self, data):
        self.df = pd.read_csv(data, encoding='unicode_escape',  error_bad_lines=False)

    # Print the data frame
    def print_df(self):
        print(self.df)

    # Null field handler for specific rows
    def null_handler(self, field):
        self.df[field] = self.df[field].replace([0], " ")
        df = self.df
        return self.df.to_csv("new_" + field + ".csv")

    # Null field handler for specific rows
    def nan_handler(self, field):
        self.df[field] = self.df[field].replace("NaN", " ")
        df = self.df
        return df.to_csv("new_" + field + ".csv")

    # Zero value in field to null text value
    def zero_handler(self, field):
        self.df[field] = self.df[field].replace(["null"], 0)
        df = self.df
        return df.to_csv("new_" + field + ".csv")

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
    def date_adv_handler(self):
        print("Please enter the date format you want in this format: %x/%y/%z example: %d/%m/%y")
        formatting = input(" please enter the format")
        field = input("Now please enter teh field you want to format:")
        self.df[field] = pd.to_datetime(self.df[field], format=formatting)
        return self.df.to_csv("new_" + field)

    # Integer to date handler
    def int_to_date(self, field):
        self.df.to_datetime(str(field), format='%Y-%m-%d')
        return self.df.to_csv("new_" + field)

    # data frame head
    def df_head(self):
        print(self.df.head())

    # data frame tail
    def df_tail(self):
        print(self.df.tail())






