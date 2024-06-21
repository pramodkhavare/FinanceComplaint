#Help to connect MongoDB 

import pymongo  #pymongo is driver which help to connect Python with MongoDB
import os 
import certifi 
from FinancialComplaints.constant import env_var 

ca = certifi.where()
mongo_client = pymongo.MongoClient(env_var.mongo_db_url ,tlsCAFile=ca)