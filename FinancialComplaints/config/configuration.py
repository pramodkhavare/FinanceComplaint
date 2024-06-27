from FinancialComplaints.logger import logging
from FinancialComplaints.exception import CustomException 
import os,sys 
from datetime import datetime


from FinancialComplaints.entity.config_entity import TrainingPipelineConfig ,DataIngestionConfig ,DataValidationConfig ,DataTransformationConfig
from FinancialComplaints.utils.utils import read_yaml
from FinancialComplaints.constant import *
from FinancialComplaints.entity.meta_data_entity import DataIngestionMetadata

class Configuration():
    def __init__(self ,config_file_path =CONFIG_FILE_PATH ,
                 current_time_stamp = CURRENT_TIME_STAMP):
        
        try:
            # self.config_info = read_yaml(yaml_file_path= config_file_path)
            self.config_info = read_yaml(r'D:\Data Science\MachineLearning\Project\FinancialInstituePrediction\config\config.yaml')
            self.training_pipeline_config = self.get_training_pipeline_config()
            self.time_stamp = current_time_stamp

        except Exception as e:
            raise CustomException (e ,sys)
        
    
    def get_data_ingestion_config(self) ->DataIngestionConfig:
        try:

            config = self.config_info[DATA_INGESTION_CONFIG_KEY]

            #min_start_date is fixed in config.yaml(Fixed)
            min_start_date = datetime.strptime(config[DATA_INGESTION_MIN_START_DATE_KEY] ,"%Y-%m-%d")

            #We will pass from_date as input to the function
            from_date = config[DATA_INGESTION_FROM_DATE_KEY]
            from_date_ = datetime.strptime( from_date ,"%Y-%m-%d")

            if from_date_ < min_start_date:
                from_date = min_start_date

            to_date = config[DATA_INGESTION_TO_DATE_KEY] 

            if to_date is None:
                to_date = datetime.now().strftime("%Y-%m-%d")
            
            

            data_ingestion_dir_key = os.path.join(
                (self.training_pipeline_config.artifacts_dir)  ,
                config[DATA_INGESTION_DIR_NAME_KEY]
            )

            metadata_file_path = os.path.join(data_ingestion_dir_key ,config[DATA_INGESTION_METADATA_FILE_NAME_KEY])

            data_ingestion_metadata = DataIngestionMetadata(metadata_file_path= metadata_file_path)

            if data_ingestion_metadata.is_metadata_file_present:
                metadat_info = data_ingestion_metadata.get_metadata_info()
                from_date_ = metadat_info.to_date

            download_dir =os.path.join(data_ingestion_dir_key ,CURRENT_TIME_STAMP , config[DATA_INGESTION_DOWNLOADED_DATA_DIR_NAME_KEY])
            filename = config[DATA_INGESTION_FILE_NAME_KEY]

            feature_store_dir = os.path.join(data_ingestion_dir_key , CURRENT_TIME_STAMP , config[DATA_INGESTION_FEATURE_STORE_DIR_NAME_KEY] )
            datasource_url = config[DATA_INGESTION_DATA_STORE_URL_KEY]

            failed_dir = os.path.join(data_ingestion_dir_key ,CURRENT_TIME_STAMP , config[DATA_INGESTION_FAILED_DIR_NAME_KEY])
            data_ingestion_config = DataIngestionConfig(
                from_date = from_date , 
                to_date = to_date ,
                data_ingestion_dir =data_ingestion_dir_key,
                download_dir =download_dir ,
                failed_dir =failed_dir,
                file_name =filename,
                feature_store_dir =feature_store_dir ,
                metadata_file_path =metadata_file_path,
                datasource_url =datasource_url
            ) 
            return data_ingestion_config

        except Exception as e:
            raise CustomException (e ,sys) from e 
        
    def get_data_validation_config(self) ->DataValidationConfig:
        try:
            config = self.config_info[DATA_VALIDATION_CONFIG]

            schema_file_path = SCHEMA_FILE_PATH 

            data_validation_dir = os.path.join((self.training_pipeline_config.artifacts_dir),
                                             config[DATA_VALIDATION_DIR_NAME_KEY],
                                             )
            accepted_data_dir = os.path.join(data_validation_dir ,config[DATA_VALIDATION_ACCEPTED_DATA_DIR_KEY] ,CURRENT_TIME_STAMP)

            rejected_data_dir = os.path.join(data_validation_dir ,config[DATA_VALIDATION_REJECTED_DATA_DIR_KEY] ,CURRENT_TIME_STAMP) 

            file_name = config[DATA_VALIDATION_FILE_NAME_KEY]    

            data_validation_config = DataValidationConfig(
                schema_file_path= schema_file_path,
                accepted_data_dir= accepted_data_dir,
                rejected_data_dir= rejected_data_dir,
                file_name= file_name
            )

            return data_validation_config
        except Exception as e:
            raise CustomException (e ,sys)
        
    def get_data_transformation(self) ->DataTransformationConfig:
        try:
            
            config = self.config_info[DATA_TRANSFORMATION_CONFIG_KEY]
            add_days_to_shipment = config[ADD_DATA_COLUMN]
            transformed_data_dir = os.path.join((self.training_pipeline_config.artifacts_dir),
                                             config[TRANSFORMED_DIR_KEY],
                                             )
            transformed_data_file = config[TRANSFORMED_DATA_FILE_NAME_KEY] 
            preprocessed_object_dir = os.path.join(
                transformed_data_dir ,
                config[PREPROCESSING_DIR_KEY]
            )
            
            preprocessing_object_file_name = config[TRANSFORMED_DATA_FILE_NAME_KEY]
            
            data_transformation_config = DataTransformationConfig(
                add_days_to_shipment= add_days_to_shipment,
                transformed_data_dir= transformed_data_dir,
                transformed_data_file = transformed_data_file ,
                preprocessed_object_dir= preprocessed_object_dir,
                preprocessing_object_file_name= preprocessing_object_file_name
            ) 
            
            return data_transformation_config
        except Exception as e:
            raise CustomException (e ,sys)
    def get_training_pipeline_config(self) ->TrainingPipelineConfig:
        try:
        
            config = self.config_info[TRAINING_PIPELINE_CONFIG]
            artifact_dir  = os.path.join(ROOT_DIR 
                                         ,config[TRAINING_PIPELINE_CONFIG_ARTIFACTS_DIR] ,
                                        )
            
            training_pipeline_config = TrainingPipelineConfig(artifacts_dir=artifact_dir)
            return training_pipeline_config

        except Exception as e:
            raise CustomException (e ,sys)