from dataclasses import dataclass 

@dataclass
class DataIngestionArtifact:
    feature_store_file_path :str 
    metadata_file_path :str 
    download_dir :str 


@dataclass(frozen=True)
class DataValidationArtifacts:
    accepted_data_dir :str 
    rejected_data_dir :str
    report_file_path :str 
    is_validated :str 
    

   
@dataclass(frozen=True)
class DataTransformationArtifacts:
    is_transformed :bool 
    transformed_data_file_path :str 
    preprocessing_obj_file_path :str 