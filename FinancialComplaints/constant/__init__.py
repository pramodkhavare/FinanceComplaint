from dataclasses import dataclass
from datetime import datetime
import os,sys 

@dataclass
class EnvironmentVariables:
    mongo_db_url = os.getenv('MONGO_DB_URL')

env_var = EnvironmentVariables()


CURRENT_TIME_STAMP = f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
ROOT_DIR = os.getcwd()  



CONFIG_DIR = "config"
CONFIG_FILE_NAME = "config.yaml"
SCHEMA_FILE_NAME = 'schema.yaml'
CONFIG_FILE_PATH = os.path.join(ROOT_DIR , CONFIG_DIR ,CONFIG_FILE_NAME)
SCHEMA_FILE_PATH = os.path.join(ROOT_DIR , CONFIG_DIR ,SCHEMA_FILE_NAME)

#Hard Coded variable related with training pipeline
TRAINING_PIPELINE_CONFIG = 'training_pipeline_config' 
TRAINING_PIPELINE_CONFIG_ARTIFACTS_DIR = 'artifact_dir'


#Variable Related With Data Ingestion
DATA_INGESTION_CONFIG_KEY = 'data_ingestion_config'
DATA_INGESTION_DIR_NAME_KEY = 'data_ingestion_dir_name'
DATA_INGESTION_DOWNLOADED_DATA_DIR_NAME_KEY = 'data_ingestion_downloaded_data_dir_name'
DATA_INGESTION_FILE_NAME_KEY = 'data_ingestion_file_name'
DATA_INGESTION_FEATURE_STORE_DIR_NAME_KEY = 'data_ingestion_feature_store_dir_name'
DATA_INGESTION_FAILED_DIR_NAME_KEY = 'data_ingestion_failed_dir_name'
DATA_INGESTION_METADATA_FILE_NAME_KEY ='data_ingestion_metadata_file_name'
DATA_INGESTION_MIN_START_DATE_KEY = 'data_ingestion_min_start_date'
DATA_INGESTION_DATA_STORE_URL_KEY ='data_ingestion_data_store_url' 
DATA_INGESTION_FROM_DATE_KEY = 'data_ingestion_from_date'
DATA_INGESTION_TO_DATE_KEY = 'data_ingestion_to_date' 


#Variable Related With Data Validation
DATA_VALIDATION_CONFIG = "data_validation_config"
DATA_VALIDATION_DIR_NAME_KEY = 'data_validation_dir_name'
DATA_VALIDATION_ACCEPTED_DATA_DIR_KEY = 'data_validation_accepted_data_dir'
DATA_VALIDATION_REJECTED_DATA_DIR_KEY = 'data_validation_rejected_data_dir'
DATA_VALIDATION_FILE_NAME_KEY = 'data_validation_folder_name'
DATA_VALIDATION_FILE_NAME_KEY = 'data_validation_file_name'


#VARIABLE RELATED WITH DATA TRANSFORMATION_CONFIG 
DATA_TRANSFORMATION_CONFIG_KEY = 'data_transformation_config'
ADD_DATA_COLUMN = 'add_date_column'
TRANSFORMED_DIR_KEY = 'transformed_dir'
PREPROCESSING_DIR_KEY ='preprocessing_dir'
PREPROCESSING_OBJECT_FILE_NAME_KEY = 'preprocessing_object_file_name'
TRANSFORMED_TRAIN_DIR_KEY = 'transformed_train_dir'
TRANSFORMED_TEST_DIR_KEY = 'transformed_test_dir'
TRANSFORMED_DATA_FILE_NAME_KEY ='transformed_data_file'