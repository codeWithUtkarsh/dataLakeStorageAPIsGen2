from flask import Flask
from flask import request
import datalakeclient as con
import configs
import uuid
from metaclient import MetaStorageClient


app = Flask(__name__)

@app.route('/persist', methods = ['POST'])
def postJsonHandler():
    
    content = request.get_json()
    images = content['images']
    ssc_number=content['ssc_meta']['ssc_number'] 
    customer_number=content['customer_meta']['customer_number']
    
    try:  
        global service_client
        storage_account_name=configs.storage_account_name
        storage_account_key=configs.storage_account_key

        service_client = con.DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

        for image in images:
            folder = customer_number +"/"+ssc_number
            file_name =str( uuid.uuid1() )
            path_for_cosmos="https://" + storage_account_name+".blob.core.windows.net/"+ configs.file_system +"/"+folder + "/" +file_name
            msc = MetaStorageClient(configs.table_name,configs.storage_account_name,configs.storage_account_key)
            msc.upload(path_for_cosmos,ssc_number,customer_number,{}) 
            con.upload_file_to_directory(service_client,image,folder,file_name)        
        
    except Exception as e:
        print(e)
    return 'JSON posted'
  
app.run(host='0.0.0.0', port= 8090)