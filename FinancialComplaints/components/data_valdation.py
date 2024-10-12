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
from dataclasses import dataclass
from typing import List, Dict


class MisingReport:
    def __init__(self ,total_rows ,missing_rows ,missing_percentage):
        self.total_rows = total_rows 
        self.missing_rows = missing_rows 
        self.missing_percentage = missing_percentage 
        
    def __repr__(self):
        return f"MissingReport(total_row={self.total_rows}, missing_row={self.missing_rows}, missing_percentage={self.missing_percentage})"

# @dataclass
# class MisingReport:
#     total_rows : int
#     missing_rows : int  
#     missing_percentage : float
        


class DataValidation:
    def __init__(self ,data_validation_config:DataValidationConfig ,
                 data_ingestion_artifacts : DataIngestionArtifact ):
        try:
            logging.info(f'\n\n{"*" * 20} Data Validation Step Started {"*" *20}') 
            self.data_validation_config  = data_validation_config
            self.data_ingestion_artifacts = data_ingestion_artifacts
            schema_file_path = SCHEMA_FILE_PATH 
            self.schema_file_content = read_yaml(schema_file_path)

        except Exception as e:
            raise CustomException(e ,sys) from e 
        
    def check_file_exist(self ,filepath)->bool:
        try:
            logging.info('Checking file exist or not') 
            is_file_exist = False 
            is_file_exist = os.path.exists(filepath)
            file_path = filepath
            if not is_file_exist:
                logging.info("We Cant procees because file is not available")
                raise Exception(f"Training  File :[{file_path}] is not available")
            return is_file_exist
        except Exception as e:
            raise CustomException(e ,sys) from e  
        
    def validate_dataset_schema(self ,dataframe ,list_of_columns:List )->bool:
        try:

            logging.info('Checking columns of train and test file') 
            validation_status = False 
            # dataframe = pd.read_csv(self.data_ingestion_artifacts.feature_store_file_path)

            actual_file_columns = list(dataframe.columns)

            validation_status = all(element in list_of_columns for element in actual_file_columns)

            logging.info(f'Checked columns  file : [{validation_status}]')
            
            
            return validation_status 
        except Exception as e:
            raise CustomException(e ,sys) from e 
        
        
  
    def get_missing_report(self ,dataframe):
        try:
            number_of_rows = len(dataframe) 
            missing_report = dict()
            
            for column in dataframe.columns:
                missing_row = dataframe[column].isnull().sum()
                missing_percent = (missing_row*100)/number_of_rows 
                missing_report[column] = MisingReport(
                    total_rows= number_of_rows ,
                    missing_rows= missing_row ,
                    missing_percentage= missing_percent
                )
                
            logging.info(f"Missing report prepared: {missing_report}")
            
            # Save the missing report to a JSON file
            report_file_dir = os.path.dirname(self.data_validation_config.accepted_data_dir)
            os.makedirs(report_file_dir ,exist_ok=True)
            
            report_file_path = os.path.join(
                report_file_dir ,
                'report.json'
            )
            
            serializable_missing_report = {
                column: {
                "total_rows": report.total_rows,
                "missing_rows": report.missing_rows,
                "missing_percentage": report.missing_percentage
                }  for column, report in missing_report.items()
                }
            
            print(serializable_missing_report)
            # with open(report_file_path, 'w') as outfile:
            #     json.dump(serializable_missing_report, outfile, indent=4)
            # logging.info(f"Missing report saved to: {report_file_path}")
            
            return missing_report
        
        except Exception as e:
            raise CustomException(e ,sys) from e  
    
    
    
    def get_unwanted_and_high_missing_value_count(self ,dataframe ,treshold =0.2) :
        try:
            missing_report = self.get_missing_report(dataframe=dataframe) 
            unwaned_column = self.schema_file_content['unwanted_column']
            unwaned_column :List[str] = list(unwaned_column.split()) 
            
            for column in missing_report:
                if missing_report[column].missing_percentage > treshold*100:
                    unwaned_column.append(column)
                    logging.info(f'We need to drop columns {unwaned_column}')
            unwanted_column = set(unwaned_column)
            
            return unwanted_column
        except Exception as e:
            raise CustomException(e ,sys) from e 
        
    def drop_unwanted_columns(self ,dataframe):
        try:
            unwanted_columns = self.get_unwanted_and_high_missing_value_count(dataframe=dataframe)
            # print(unwanted_columns)
            # print(dataframe.columns)
            dataframe = dataframe.drop(columns=unwanted_columns)
            # print(dataframe.columns)
            logging.info(f"After dropping unwanted column remaining columns are {dataframe.columns}")
            return dataframe
        except Exception as e:
            raise CustomException(e ,sys) from e 
        
    
        
    def initiate_data_validation(self):
        try:
    
            accepted_data_dir = self.data_validation_config.accepted_data_dir 
            rejected_data_dir = self.data_validation_config.rejected_data_dir 
            input_file_path = self.data_ingestion_artifacts.feature_store_file_path

            check_file_exist = self.check_file_exist(filepath=input_file_path)
            
            if check_file_exist:
                overall_data = pd.read_csv(input_file_path)
                expecting_columns = list(self.schema_file_content['columns'].keys())
                if self.validate_dataset_schema(dataframe=overall_data ,list_of_columns=expecting_columns):
                    data_after_drop_unwanted_column = self.drop_unwanted_columns(dataframe=overall_data)
                    wanted_column = list(self.schema_file_content['wanted_column'])
                    if self.validate_dataset_schema(dataframe=data_after_drop_unwanted_column ,
                                                    list_of_columns=wanted_column):
                        os.makedirs(accepted_data_dir ,exist_ok=True)
                        output_file_path = os.path.join(accepted_data_dir , self.data_validation_config.file_name)
                        data_after_drop_unwanted_column.to_csv(output_file_path)
                        logging.info("We can procees because there is no mismatch in required columns and available columns")
                        
                    else:
                        os.makedirs(rejected_data_dir ,exist_ok=True)
                        output_file_path = os.path.join(rejected_data_dir , self.data_validation_config.file_name)
                        data_after_drop_unwanted_column.to_csv(output_file_path)
                        logging.info("We cant procees because wanted columns are not in data")
                        
                    
                else:
                    os.makedirs(rejected_data_dir ,exist_ok=True)
                    output_file_path = os.path.join(rejected_data_dir , self.data_validation_config.file_name)
                    overall_data.to_csv(output_file_path)
                    logging.info("We cant procees because there is mismatch in required columns and available columns")
                    
            else:
                os.makedirs(rejected_data_dir ,exist_ok=True)
                raise CustomException(e,sys) from e
                
   
            data_validation_artifacts = DataValidationArtifacts(
                accepted_data_dir = accepted_data_dir ,
                rejected_data_dir = rejected_data_dir ,
                report_file_path = "Pass" ,
                is_validated = 'data_validation'
            )
            return data_validation_artifacts
        except Exception as e:
            raise CustomException(e,sys) from e
