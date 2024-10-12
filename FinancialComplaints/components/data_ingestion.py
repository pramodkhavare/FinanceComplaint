from FinancialComplaints.exception import CustomException 
from FinancialComplaints.logger import logging 
from dataclasses import dataclass 
from datetime import datetime
from FinancialComplaints.config.configuration import DataIngestionConfig 
from FinancialComplaints.entity.artifacts_entity import DataIngestionArtifact 
from FinancialComplaints.entity.meta_data_entity import DataIngestionMetadata 
# from FinancialComplaints.config.spark_manager import spark_session
import os,sys ,json ,re 
from time import time
import pandas as pd
import requests 


@dataclass(frozen=True)
class DownloadUrl:
    url :str 
    file_path :str 
    n_retry: int 


class DataIngestion:
    def __init__(self ,data_ingestion_config: DataIngestionConfig ,
                 n_retry: int =5):
        try:
            logging.info(f"{'>>' *20} Starting Data Ingestion {'<<'*20}")
            self.data_ingestion_config = data_ingestion_config
            self.n_retry = n_retry


        except Exception as e:
            raise(CustomException(e ,sys))
        
    def get_required_interval(self):
        try:
            start_time = datetime.strptime(self.data_ingestion_config.from_date ,'%Y-%m-%d')
            end_date = datetime.strptime(self.data_ingestion_config.to_date ,'%Y-%m-%d')
            n_dff_days = (end_date-start_time).days  
            freq =None 
            if n_dff_days > 365:
                freq = "Y"
            elif n_dff_days >30:
                freq = "ME"
            elif n_dff_days >7:
                freq= 'W'
            logging.info(f"{n_dff_days} hence freq: {freq}")

            if freq is None:
                intervels = pd.date_range(start= self.data_ingestion_config.from_date ,
                                          end= self.data_ingestion_config.to_date ,
                                          periods=2).astype('str').to_list()
            else:
                intervels = pd.date_range(start= self.data_ingestion_config.from_date ,
                                          end= self.data_ingestion_config.to_date ,
                                          freq=freq).astype('str').to_list()
                


            logging.info(f"Prepared Interval {intervels}")

            if self.data_ingestion_config.to_date not in intervels:
                intervels.append(self.data_ingestion_config.to_date)
            

            return intervels 
        
        except Exception as e:
            raise(CustomException(e ,sys))
    
    def download_data(self ,download_url :DownloadUrl):
        try:
            
            logging.info("Downloading Data Process Started")
            download_dir = os.path.dirname(download_url.file_path)

            os.makedirs(download_dir ,exist_ok= True)
            data = requests.get(url=download_url.url)
            try:
                if data.status_code != 200:
                    raise Exception(f"Failed to download data. Status code: {data.status_code}")
                
                # Parse the JSON content
                json_content = json.loads(data.content)
                finance_complaints_data = [x['_source'] for x in json_content if '_source' in x]

                with open(download_url.file_path, 'w') as file_obj:
                    json.dump(finance_complaints_data, file_obj)
                logging.info(f'Downloading if data completed and stored in {download_url.file_path}') 

            except Exception as e:
                logging.info('Failed TO Donload Data hence we will retry one more time')
                if os.path.exists(download_url.file_path):
                    os.remove(download_url.file_path)
                self.retry_download_data(data ,download_url =download_url)
            
        except Exception as e:
            raise(CustomException(e ,sys))
    
    def retry_download_data(self, data, download_url: DownloadUrl):

        """
        This function help to avoid failure as it help to download failed file again
        
        data:failed response
        download_url: DownloadUrl
        """
        try:
            # if retry still possible try else return the response
            if download_url.n_retry == 0:
                self.failed_download_urls.append(download_url)
                logging.info(f"Unable to download file {download_url.url}")
                return

            # to handle throatling requestion and can be slove if we wait for some second.
            content = data.content.decode("utf-8")
            wait_second = re.findall(r'\d+', content)

            if len(wait_second) > 0:
                time.sleep(int(wait_second[0]) + 2)

            # Writing response to understand why request was failed
            failed_file_path = os.path.join(self.data_ingestion_config.failed_dir,
                                            os.path.basename(download_url.file_path))
            os.makedirs(self.data_ingestion_config.failed_dir, exist_ok=True)
            with open(failed_file_path, "wb") as file_obj:
                file_obj.write(data.content)

            # calling download function again to retry
            download_url = DownloadUrl(download_url.url, file_path=download_url.file_path,
                                       n_retry=download_url.n_retry - 1)
            self.download_data(download_url=download_url)
        except Exception as e:
            raise CustomException(e, sys)
        
    def download_files(self ,n_day_interval_url:int =None):
        try:
            interval = self.get_required_interval()
        
            
            logging.info("Data Downloading is started")

            for index in range(1,len(interval)):
                from_date ,to_date = interval[index-1] ,interval[index]
                dataset_url = self.data_ingestion_config.datasource_url 
                
                
                url = dataset_url.replace('<todate>' , to_date).replace('<fromdate>' ,from_date)
                logging.info(url)

                # file_name = f"{self.data_ingestion_config.file_name}_{from_date}_{to_date}.json"
                file_name = f"_{from_date}_{to_date}.json"  
                file_path = os.path.join(self.data_ingestion_config.download_dir ,file_name)
                download_url = DownloadUrl(url=url ,file_path= file_path ,n_retry=self.n_retry)

                self.download_data(download_url=download_url) 

            logging.info('Downloading Step Has been completed')
        except Exception as e:
            raise(CustomException(e ,sys))
    
    def convert_files_to_parquet(self)->str:
        """
            Downloaded files will converted and merged into single parquet file 
            json_data_dir : downloaded json file directory 
            data_dir : converted and  
            output_file_name : output file name 
            """ 
        try:
            json_data_dir = self.data_ingestion_config.download_dir 
            data_dir = self.data_ingestion_config.feature_store_dir 
            output_file_name = self.data_ingestion_config.file_name 

            output_file_path = os.path.join(data_dir , output_file_name)

            os.makedirs(data_dir ,exist_ok=True)
            logging.info(f"Parquet file will store at {output_file_path}")

            if not os.path.exists(json_data_dir):
                return output_file_path 
            for file in os.listdir(json_data_dir):
                json_file_path = os.path.join(json_data_dir ,file)
                logging.info(f'Converting json file at {json_file_path}')
                df_spark = spark_session.read.json(json_file_path)
                df_spark.write.mode('append').parquet(output_file_path)


            return output_file_path

        except Exception as e:
            raise(CustomException(e ,sys))
    
    def convert_data_files_to_csv(self):
        try:
            json_data_dir = self.data_ingestion_config.download_dir 
            data_dir = self.data_ingestion_config.feature_store_dir 
            output_file_name = self.data_ingestion_config.file_name  

            output_file_path = os.path.join(data_dir , output_file_name)

            os.makedirs(data_dir ,exist_ok=True) 
            logging.info(f"csv Parquet file will store at {output_file_path}")

            if not os.path.exists(json_data_dir):
                return output_file_path 
            
            dataframes = []
            # Loop through each file in the directory
            for file_name in os.listdir(json_data_dir):
                if file_name.endswith('.json'):
                    
                    file_path = os.path.join(json_data_dir, file_name)

                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        # Convert JSON data to dataframe
                        df = pd.json_normalize(data)
                        dataframes.append(df)

            # Concatenate all dataframes

            combined_df = pd.concat(dataframes, ignore_index=True)

            # Write to CSV file
            combined_df.to_csv(output_file_path, index=False)
            return output_file_path
        except Exception as e:
            raise(CustomException(e ,sys))

    def write_metadata(self ,file_path:str):
        try:
            metadata_innfo = DataIngestionMetadata(metadata_file_path=self.data_ingestion_config.metadata_file_path)


            metadata_innfo.write_metadata_info(
                from_date=self.data_ingestion_config.from_date ,
                to_date= self.data_ingestion_config.to_date ,
                data_file_path= file_path
            )
            logging.info('MEta Data File aHas been written')
            # return metadata_innfo


        except Exception as e:
            raise(CustomException(e ,sys)) from e 
        
    def initiate_dataingestion(self):
        try:
            logging.info('Initiated Data Ingestion')
            if self.data_ingestion_config.from_date != self.data_ingestion_config.to_date:
                self.download_files()

            if os.path.exists(self.data_ingestion_config.download_dir):
                # file_path = self.convert_files_to_parquet()
                file_path = self.convert_data_files_to_csv()
                self.write_metadata(file_path=file_path)

            feature_store_file_path = os.path.join(
                self.data_ingestion_config.feature_store_dir ,
                self.data_ingestion_config.file_name  
            )

            data_ingeston_artifatcs = DataIngestionArtifact(
                feature_store_file_path=feature_store_file_path ,
                metadata_file_path= self.data_ingestion_config.metadata_file_path ,
                download_dir= self.data_ingestion_config.download_dir
            )
            return data_ingeston_artifatcs

        except Exception as e:
            raise(CustomException(e ,sys)) from e 