from FinancialComplaints.exception import CustomException 
from FinancialComplaints.logger import logging 
from FinancialComplaints.utils.utils import read_yaml ,write_yaml
from dataclasses import dataclass 
from pathlib import Path 
import os ,sys



@dataclass
class DataIngestionMetadataInfo:
    from_date : str 
    to_date :str 
    data_file_path :str 


class DataIngestionMetadata:
    def __init__(self ,metadata_file_path):
        self.metadata_file_path = metadata_file_path

    @property 
    def is_metadata_file_present(self)->bool:
        return os.path.exists(self.metadata_file_path)
    
    def get_metadata_info(self)->DataIngestionMetadataInfo:
        try:
            metadata_info = DataIngestionMetadataInfo(
                from_date= from_date ,
                to_date= to_date ,
                data_file_path= data_file_path
            )

            write_yaml(file_path=self.metadata_file_path ,data=metadata_info.)

        except Exception as e:
            raise CustomException(e ,sys)