from dataclasses import dataclass
from datetime import datetime
import os,sys 

CURRENT_TIME_STAMP = f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
ROOT_DIR = os.getcwd()  

@dataclass
class EnvironmentVariables:
    mongo_db_url = os.getenv('MONGO_DB_URL')



env_var = EnvironmentVariables()