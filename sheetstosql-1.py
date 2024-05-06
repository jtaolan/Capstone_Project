""" 
python code to process excel files and save them to MySQL database 
"""
import os, sys
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import glob
import os 
from pandas.io import sql
from sqlalchemy import create_engine, text

# Create an engine for MySQL using PyMySQL
user = input("Enter your MySQL username: ")
password = input("Enter your MySQL password: ")
# engine = create_engine("mysql+pymysql://test_user:123!@localhost/test_database")
base_engine = create_engine(f"mysql+pymysql://{user}:{password}@localhost/")


def my_create_database(base_engine, database_name):
    """ 
    Create a new database if it does not exist
    then return the engine for the new database
    """
    con = base_engine.connect()
    cmd = text(f"CREATE DATABASE IF NOT EXISTS `{database_name}`;")
    try:
        con.execute(cmd)
    except Exception as err:
        pass
    con.close()
    db = create_engine(f"mysql+pymysql://{user}:{password}@localhost/{database_name}")

    return db


""" the main function to process the excel files and write them to mysql database """
if __name__ == "__main__":
    dataset_dir = "./datasets/" #'/Users/jiaohaidediannao/Downloads/datasets/'
    logging_dir = "./logs/"
    if not os.path.exists(logging_dir):
        os.makedirs(logging_dir)

    xls_file_list = sorted(glob.glob(os.path.join(dataset_dir, "**/*.xlsx"), 
                                     recursive=True))
    # print(xls_file_list)

    for xls_file in xls_file_list:
        xls_file_name = os.path.basename(xls_file).split(".")[0]
        # read excel sheets to data frames
        try:
            xls = pd.ExcelFile(xls_file)
            print("=============== Processing file: ", xls_file)
        except Exception as err:
            # log the errors to a file
            with open("./logs/error_log_file.txt", "a") as f:
                f.write(f"Error reading file: {xls_file}\n")
                f.write(f"Error: {err}\n")
            continue

        # create a new database based on the excel file name
        db = my_create_database(base_engine, xls_file_name)

        # write each sheet to a table in the database
        sheet_names = xls.sheet_names
        for sheet_name in sheet_names:
            # read excel sheet to data frame
            df = pd.read_excel(xls, sheet_name)
            
            # data frames to sql tables one by one
            if not df.empty:
                print("Processing sheet: ", sheet_name)
                try:
                    df.to_sql(sheet_name, con=db, if_exists='replace', index=False)
                except Exception as err:
                    # log the errors to a file
                    with open("./logs/error_log_sheets.txt", "a") as f:
                        f.write(f"Error in sheet in file: {sheet_name, xls_file}\n")
                        f.write(f"Error: {err}\n")
                    continue
        
        
