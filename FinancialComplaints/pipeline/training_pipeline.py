import os ,sys
import pandas as pd
import uuid
from threading import Thread
from collections import namedtuple
from datetime import datetime 


from FinancialComplaints.config.configuration import Configuration
from FinancialComplaints.components.data_ingestion import DataIngestion 
from FinancialComplaints.components.data_valdation import DataValidation 
from FinancialComplaints.components.data_transformation import DataTransformation
from FinancialComplaints.constant import * 
from FinancialComplaints.logger import logging 
from FinancialComplaints.exception import CustomException 
from FinancialComplaints.entity.artifacts_entity import DataIngestionArtifact, DataValidationArtifacts



class Pipeline():
    def __init__(self ,config:Configuration=Configuration()):
        try:
            
            self.config = config
            os.makedirs(self.config.get_training_pipeline_config().artifacts_dir ,exist_ok=True)

        except Exception as e:
            raise CustomException(e,sys) from e
    
    def start_data_ingestion(self) ->DataIngestionArtifact:
        try:
            data_ingestion_config = self.config.get_data_ingestion_config(
            )
            data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)

            data_ingestion_artifacts = data_ingestion.initiate_dataingestion()

            return data_ingestion_artifacts
            

        except Exception as e:
            raise CustomException(e,sys) from e
    def start_data_validation(self ,data_ingestion_artifacts: DataIngestionArtifact) ->DataValidationArtifacts:
        try:
            data_validation_config = self.config.get_data_validation_config()
            data_ingestion_artifacts = data_ingestion_artifacts 
            data_validation =DataValidation(data_validation_config=data_validation_config ,
                                            data_ingestion_artifacts= data_ingestion_artifacts)
            data_validation_artifacts = data_validation.initiate_data_validation()
            return data_validation_artifacts

        except Exception as e:
            raise CustomException(e,sys) from e
        
    def start_data_transformation(self ,data_ingestion_artifacts : DataIngestionArtifact , data_validation_artifacts:DataValidationArtifacts):
        try:
            data_transformation_config = self.config.get_data_transformation()
            data_ingestion_artifacts = data_ingestion_artifacts 
            data_validation_artifacts = data_validation_artifacts
            data_transformation = DataTransformation(
                data_transformation_config= data_transformation_config,
                data_ingestion_artifacts= data_ingestion_artifacts,
                data_validation_artifacts= data_validation_artifacts
            )
            data_transformation_artifacts =data_transformation.initiate_data_transformation()
            return data_transformation_artifacts
        except Exception as e:
            raise CustomException(e,sys) from e
    def run_pipeline(self):
        try:
            pipeline = Pipeline()
            data_ingestion_artifacts = pipeline.start_data_ingestion()
            data_validation_artifacts = pipeline.start_data_validation(data_ingestion_artifacts=data_ingestion_artifacts)
            # data_transformation_artifacts = pipeline.start_data_transformation(data_ingestion_artifacts=data_ingestion_artifacts ,data_validation_artifacts=data_validation_artifacts)
        except Exception as e:
            raise CustomException(e,sys) from e
        
    def run(self):
        try:
            self.run_pipeline()

        except Exception as e:
            raise CustomException(e ,sys) from e

    # D:\Data Science\MachineLearning\Project\FinancialInstituePrediction\FinancialComplaints\pipeline\training_pipeline.py