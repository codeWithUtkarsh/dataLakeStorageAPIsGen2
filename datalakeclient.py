import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
import configs

def upload_file_to_directory(service_client,file_value,folder,file_name):
    try:

        file_system_client = service_client.get_file_system_client(file_system=configs.file_system)
        directory_client = file_system_client.get_directory_client(folder)
        print(directory_client)
        file_client = directory_client.create_file(file_name)
        file_client.append_data(data=file_value, offset=0, length=len(file_value))
        file_client.flush_data(len(file_value))

    except Exception as e:
      print(e)    