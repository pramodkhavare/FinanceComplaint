import os ,sys 
import pandas as pd
from FinancialComplaints.utils.utils import read_yaml ,write_yaml ,check_lists_match 
from FinancialComplaints.config.configuration import DataValidationConfig 
from FinancialComplaints.constant import * 
from FinancialComplaints.exception import CustomException
from FinancialComplaints.logger import logging
from FinancialComplaints.entity.artifacts_entity import DataValidationArtifacts ,DataIngestionArtifact
# Import the necessary modules
# from evidently.model_profile import Profile
# from evidently.model_profile.sections import DataDriftProfileSection
# from evidently.dashboard import Dashboard 
# from evidently.dashboard.tabs import DataDriftTab
import pandas as pd
import json 
from evidently.report import Report
import shutil


class DataValidation:
    def __init__(self ,data_validation_config:DataValidationConfig ,
                 data_ingestion_artifacts : DataIngestionArtifact ):
        try:
            logging.info(f'\n\n{"*" * 20} Data Validation Step Started {"*" *20}') 
            self.data_validation_config  = data_validation_config
            self.data_ingestion_artifacts = data_ingestion_artifacts


        except Exception as e:
            raise CustomException(e ,sys) from e 
        
    def check_file_exist(self)->bool:
        try:
            logging.info('Checking file exist or not') 
            is_file_exist = False 
            is_file_exist = os.path.exists(self.data_ingestion_artifacts.feature_store_file_path)
            file_path = self.data_ingestion_artifacts.feature_store_file_path
            if not is_file_exist:
                logging.info("We Cant procees because file is not available")
                raise Exception(f"Training  File :[{file_path}] is not available")
            return is_file_exist
        except Exception as e:
            raise CustomException(e ,sys) from e 
        
    def validate_dataset_schema(self)->bool:
        try:
            logging.info('Checking columns of train and test file') 
            validation_status = False 
            schema_file_path = SCHEMA_FILE_PATH 
            schema_file_content = read_yaml(schema_file_path)

            expecting_columns = list(schema_file_content['columns'].keys())
            target_columns =list(schema_file_content['target_columns'].keys())
            expecting_columns.append(target_columns[0])

            dataframe = pd.read_csv(self.data_ingestion_artifacts.feature_store_file_path)

            actual_file_columns = list(dataframe.columns)


            validation_status = all(element in expecting_columns for element in actual_file_columns)


            logging.info(f'Checked columns  file : [{validation_status}]')
            
            
            return validation_status  
        except Exception as e:
            raise CustomException(e ,sys) from e 
        
    def initiate_data_validation(self):
        try:
            data_validation =False 
            data_validation = self.validate_dataset_schema() and  self.check_file_exist()

            accepted_data_dir = self.data_validation_config.accepted_data_dir 
            rejected_data_dir = self.data_validation_config.rejected_data_dir 
            input_file_path = self.data_ingestion_artifacts.feature_store_file_path
   
            if data_validation:
                os.makedirs(accepted_data_dir ,exist_ok=True)
                output_file_path = os.path.join(accepted_data_dir , self.data_validation_config.file_name)
                shutil.copy2(input_file_path, output_file_path)
                logging.info("We can procees because there is no mismatch in required columns and available columns")
            else:
                os.makedirs(rejected_data_dir ,exist_ok=True)
                output_file_path = os.path.join(rejected_data_dir , self.data_validation_config.file_name)
                shutil.copy2(input_file_path, output_file_path)
                logging.info("We cant procees because there is mismatch in required columns and available columns")
   
            data_validation_artifacts = DataValidationArtifacts(
                accepted_data_dir = accepted_data_dir ,
                rejected_data_dir = rejected_data_dir ,
                report_file_path = "Pass" ,
                is_validated = data_validation
            )
            return data_validation_artifacts
        except Exception as e:
            raise CustomException(e,sys) from e
