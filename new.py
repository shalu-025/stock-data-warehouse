"""
Title: SCRIPT TO CREATE FOLDER STRUCTURE
Author: Tata Shalini
Created date: 23-11-2025
Last updated date: 23-11-2025
Purpose: Function to create the folder structure to be reused

"""


# importing libraries
from datetime import date
import os

def create_folder(name:str,target_date:date = None)->str:
    """Create folder structure based on given or current date."""
    try:
        d=target_date or date.today()
        year=str(d.year)
        month=str(d.month)
        day=str(d.day)
        path=os.path.join(os.getcwd(),name,year,month,day)
        os.makedirs(path,exist_ok=True)
        return path

    except Exception as e:
        raise e

