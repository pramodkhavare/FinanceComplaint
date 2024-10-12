"""Purpose of these file is to write down meta data """

from FinancialComplaints.exception import CustomException 
from FinancialComplaints.logger import logging 
from FinancialComplaints.utils.utils import read_yaml ,write_yaml
from dataclasses import dataclass 
from pathlib import Path 
import os ,sys



# class DataIngestionMetadataInfo():
#     def __init__(self ,from_date:str ,to_date:str ,data_file_path:str):
#         self.from_date = from_date 
#         self.to_date = to_date 
#         self.data_file_path = data_file_path 
        
#     def to_dict(self):
#         return {
#             'from_date': self.from_date,
#             'to_date': self.to_date,
#             'data_file_path': self.data_file_path
#         }

@dataclass
class DataIngestionMetadataInfo:
    from_date : str 
    to_date :str 
    data_file_path :str 


    def to_dict(self):
        return {
            'from_date': self.from_date,
            'to_date': self.to_date,
            'data_file_path': self.data_file_path
        }


class DataIngestionMetadata:
    def __init__(self ,metadata_file_path):
        self.metadata_file_path = metadata_file_path
    
    @property 
    def is_metadata_file_present(self)->bool:      #'meta_info.yaml' 
        return os.path.exists(self.metadata_file_path)
    
    def write_metadata_info(self ,from_date ,to_date ,data_file_path)->DataIngestionMetadataInfo:
        try:
            metadata_info = DataIngestionMetadataInfo(
                from_date= from_date ,
                to_date= to_date ,
                data_file_path= data_file_path
            )

            metadata_dict = metadata_info.to_dict()

            write_yaml(file_path=self.metadata_file_path ,data=metadata_dict)

        except Exception as e:
            raise CustomException(e ,sys)
        
    def get_metadata_info(self)->DataIngestionMetadataInfo:
        try:
            if not self.is_metadata_file_present:
                raise Exception('No meta Data file is available')
            metadata = read_yaml(self.metadata_file_path)
            metedata_info = DataIngestionMetadataInfo(**(metadata))
            logging.info(metadata)
            return metedata_info
        except Exception as e:
            raise CustomException(e ,sys)