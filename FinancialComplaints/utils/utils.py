from FinancialComplaints.exception import  CustomException
from FinancialComplaints.logger import logging
import sys ,os 
from ensure import ensure_annotations
import yaml
import numpy as np
import pandas as pd
import dill 
from FinancialComplaints.constant import *



# def write_yaml(file_path:str ,data:dict):
#     """
#     This function willl create Yaml file and save data in that file
#     """
#     try:
#         os.makedirs(os.path.dirname(file_path) ,exist_ok=True)
#         with open(file_path ,'w') as yaml_file:
#             if data is not None :
#                 yaml.dump(data ,yaml_file)

#     except Exception as e:
#         logging.info(f'unable to create Yaml file at {file_path}')
#         raise CustomException(e ,sys)

def write_yaml(file_path, data):
        try:
            os.makedirs(os.path.dirname(file_path) ,exist_ok=True)

            with open(file_path, 'w') as yaml_file:
                if data is not None:
                     yaml.dump(data ,yaml_file)
            logging.info(f"Metadata successfully written to {file_path}")
        except Exception as e:
            logging.error(f"Failed to write metadata to {file_path}: {e}")
            raise CustomException(e, sys)

def read_yaml(yaml_file_path:str):
    try:
        """
        Read yaml file and return content as dictionary
        yaml_file_path :str
        """
        with open(yaml_file_path , 'r') as file:
            content =  yaml.safe_load(file)
            return content

    except Exception as e:
        logging.info(f'unable to read Yaml file at {yaml_file_path}')
        raise CustomException(e ,sys)

def check_lists_match(list1 ,list2):
     try:
        if len(list1) != len(list2):
               return False 
        for i in range(len(list1)):
            if list1[i]!=list2[i]:
                return False
        return True 


     except Exception as e:
        logging.info(f'Mismatch in between Columns')
        raise CustomException(e ,sys)