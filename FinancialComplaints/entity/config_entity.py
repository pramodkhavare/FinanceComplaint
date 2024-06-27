import os ,sys 
from datetime import datetime 
from dataclasses import dataclass 
from pathlib import Path 

def get_time_stamp():
    return datetime.now().strftime('%Y-%m-%d-%H-%M-%S') 




@dataclass(frozen=True)
class TrainingPipelineConfig:
    artifacts_dir :str 

@dataclass(frozen=True)
class DataIngestionConfig:
    from_date :datetime
    to_date : datetime 
    data_ingestion_dir :str
    download_dir :str 
    file_name :str
    feature_store_dir :str 
    failed_dir :str
    metadata_file_path :str
    datasource_url :str  


@dataclass(frozen=True)
class DataValidationConfig:
    schema_file_path :str 
    accepted_data_dir :str 
    rejected_data_dir :str 
    file_name :str 

@dataclass(frozen=True)
class DataTransformationConfig:
    add_days_to_shipment : bool 
    transformed_data_dir : str 
    transformed_data_file :str
    preprocessed_object_dir : str 
    preprocessing_object_file_name :str
     



