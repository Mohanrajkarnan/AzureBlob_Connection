
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd
from AzureKeyVault_Connection import azureAccessKey
import requests,os,shutil,gzip
import ssl
from azure.core.exceptions import ResourceExistsError

ssl._create_default_https_context = ssl._create_unverified_context

#credentials
account_name = 'gsodadlsdev'
account_key = azureAccessKey("blobAccessKey")
container_name = 'gsod'

#create a client to interact with blob storage
connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

#use the client to connect to the container
container_client = blob_service_client.get_container_client(container_name)

def uploadToBlob(urlPath):
    """
    This method takes the URL as str and save GSOD data in ADLS
    """
    url = urlPath
    try:
        yearDf = pd.read_html(url)
        yearLst = yearDf[0].Name.tolist()

        for year in yearLst:
            if str(year).endswith("/"):

                downloadFile = pd.read_html(f"{url}{year}")
                fileLst = downloadFile[0].Name.tolist()
                flag = 0

                for zipFile in fileLst:
                    if str(zipFile).endswith(".gz"):
                        r = requests.get(f'{url}{year}/{zipFile}',verify=False)
                        open(zipFile, 'wb').write(r.content)

                        with gzip.open(zipFile, 'rb') as file_in:
                            flag +=1
                            unzipFileName = zipFile[:-3]
                            with open(f'{unzipFileName}', 'wb') as file_out:
                                shutil.copyfileobj(file_in, file_out)
                            
                            #Uploading into blob
                            with open(f'{unzipFileName}', "r") as fl :
                                try:
                                    data = fl.read()
                                    container_client.upload_blob(name = f"{year}/{unzipFileName}", data=data,overwrite=False)
                                except ResourceExistsError :
                                    print(f"{unzipFileName} is already exists")

                        os.remove(f'{unzipFileName}')
                        os.remove(zipFile)
                        if flag >= 10:
                                break
    except Exception as e:
        print(f"This failed because of this error : {e} ")


def readFromBlob():

    """
    This method will read the GSOD data from blob and return as pandas dataframe.
    """

    try:
        #get a list of all blob files in the container
        blob_list = []
        for blob_i in container_client.list_blobs():
            blob_list.append(blob_i.name)

        df_list = []
        #generate a shared access signiture for files and load them into Python
        for blob_i in blob_list:
            #generate a shared access signature for each blob file
            sas_i = generate_blob_sas(account_name = account_name,
                                        container_name = container_name,
                                        blob_name = blob_i,
                                        account_key=account_key,
                                        permission=BlobSasPermissions(read=True),
                                        expiry=datetime.now() + timedelta(hours=1))
            
            if str(blob_i).endswith(".op"):
                sas_url = 'https://' + account_name+'.blob.core.windows.net/' + container_name + '/' + blob_i + '?' + sas_i
                df = pd.read_fwf(sas_url)

                df_list.append(df)
        
        df_combined = pd.concat(df_list, ignore_index=True)
        return df_combined

    except Exception as e:
        print(f"This failed because of this error : {e} ") 


if __name__ == "__main__":
#    uploadToBlob('https://www1.ncdc.noaa.gov/pub/data/gsod/')
   df = readFromBlob()
   print(df)

                       
                    