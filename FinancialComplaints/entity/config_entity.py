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
    dataset_download_url :str
    tgz_download_dir : str 
    raw_data_dir :str
    ingested_dir :str 
    ingested_train_dir :str 
    ingested_test_dir :str
