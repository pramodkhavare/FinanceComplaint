from FinancialComplaints.exception import CustomException 
from FinancialComplaints.logger import logging 
from dataclasses import dataclass 
from datetime import datetime
from FinancialComplaints.config.configuration import DataIngestionConfig 
from FinancialComplaints.entity.artifacts_entity import DataIngestionArtifact ,DataValidationArtifacts ,DataTransformationArtifacts
from FinancialComplaints.entity.meta_data_entity import DataIngestionMetadata 
from FinancialComplaints.entity.config_entity import DataTransformationConfig
import os,sys ,json ,re 
from time import time
import pandas as pd
from sklearn.base import BaseEstimator,TransformerMixin 
from FinancialComplaints.utils.utils import read_yaml ,write_yaml
from sklearn.compose import ColumnTransformer
from FinancialComplaints.constant import *
from sklearn.pipeline import Pipeline 
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler 
from sklearn.preprocessing import OneHotEncoder 
from sklearn.impute import SimpleImputer 
from sklearn.preprocessing import LabelBinarizer 
from category_encoders import BinaryEncoder



class FeatureGenerator(BaseEstimator , TransformerMixin):
    def __init__(self ,add_days_to_shipment =True ,
                 date_received_ix = 0 ,
                 date_sent_ix = 7 ,
                 columns = None):

        try:
            self.columns = columns 
            if self.columns is not None: 
                date_received_ix = self.columns.index('Date received')
                date_sent_ix = self.columns.index('Date sent to company')

            self.add_date_to_shipment = add_days_to_shipment 
            self.date_received_ix = date_received_ix
            self.date_sent_ix = date_sent_ix
        except Exception as e:
            raise CustomException(e,sys) from e 
        
    def fit(self ,X ,y):
        return self 
    
    def transform(self ,X ,y=None):
        try:
            
            # X.drop(columns=['Date sent to company' ,'Date received'],axis=1 ,inplace=True) 
            if self.add_date_to_shipment:
                X['Days Difference'] = pd.to_datetime(X.iloc[: ,2]) - pd.to_datetime(X.iloc[: ,13]) 
            else:
                X 
            
            return  X

        
        except Exception as e:
            raise CustomException(e,sys) from e
        
class DataTransformation():
    def __init__(self ,
                 data_transformation_config :DataTransformationConfig ,
                 data_ingestion_artifacts :DataIngestionArtifact ,
                 data_validation_artifacts : DataValidationArtifacts):
        try:
            logging.info(f'\n\n{"*" * 20} Data Transformation Step Started {"*" *20}') 
            self.data_transformation_config = data_transformation_config
            self.data_ingestion_artifact = data_ingestion_artifacts 
            self.data_validation_artifact = data_validation_artifacts

        except Exception as e:
            raise CustomException(e,sys) from e 
        
    @staticmethod
    def check_data_type(file_path ,schem_file_path):
        try:
            data = pd.read_csv(file_path)
            schema_data = read_yaml(schem_file_path)
            schema = schema_data['columns']
            for column in data.columns:
                if column in list(schema.keys()):
                    data[column].astype(schema[column])
                else:
                    raise Exception(e,sys)
                
        except Exception as e:
            raise CustomException(e,sys) from e 
        
    def get_data_transformer_object(self) ->ColumnTransformer:
        try:
            logging.info("We are Creating Data Transformation Object") 
            schema_file_path = SCHEMA_FILE_PATH 
            dataset_schema = read_yaml(schema_file_path)

            numerical_column = dataset_schema['numerical_colum'].split(" ")
            categorical_column =dataset_schema['categorical_columns'].split(" ")
            binary_feature = ['Product' ,'State' ,'Submitted via' ,'Company response to consumer']
            onhot_feature =['Consumer consent provided?' ,'Timely response?' ,'State']


            one_hot_pipeline = Pipeline(
                steps=[
                    ('impute' ,SimpleImputer(strategy='most_frequent')) ,
                    ('feature_generator' ,FeatureGenerator()) ,
                    ('onehotencode' ,OneHotEncoder()) 
                    ]
                   )

            binary_encoder_pipeline = Pipeline(
                steps=[
                    ('impute' ,SimpleImputer(strategy='most_frequent')) ,
                    ('binaryencode' ,BinaryEncoder()) 
                    ]
                )

            num_pipeline = Pipeline(
                    steps=[
                        ('impute' ,SimpleImputer(strategy='median')),
                        ('scalar' ,StandardScaler())
                    ]
                )

            preprocessor = ColumnTransformer([
                    ('num_pipeline' ,num_pipeline ,numerical_column) ,
                    ('one_hot_pipeline' ,one_hot_pipeline ,onhot_feature)  ,
                    ('binary_encoder_pipeline' ,binary_encoder_pipeline ,binary_feature)
                ])
            
            
            return preprocessor

        except Exception as e:
            raise CustomException(e,sys) from e 
        
    
    
    def initiated_data_transformation(self) ->DataTransformationArtifacts:
        try:
            logging.info(f'{"*"*20}Data Transformation started {"*"*20}')
            preprocessing_obj = self.get_data_transformer_object()
            logging.info(f'Loading Data from folder {self.data_ingestion_artifact} and {self.data_ingestion_artifact.feature_store_file_path}')
            folder  = self.data_validation_artifact.accepted_data_dir
            file_path = os.path.join(folder , os.listdir(folder)[0])
            data = pd.read_csv(file_path)
            
            schema_file_path = SCHEMA_FILE_PATH 
            schema = read_yaml(schema_file_path)
            
            target_column_name = schema['target_columns']
            
            input_data = data.drop(columns=[target_column_name])
            input_data.drop(columns=['Tags' ,'Complaint ID' ,'ZIP code' ,'Sub-product' ,'Sub-issue' ,'Company public response' ,'Consumer complaint narrative' ] ,inplace=True)
            input_data = preprocessing_obj.fit_transform(input_data)
            print(input_data)
  
        except Exception as e:
            raise CustomException(e,sys) from e 


