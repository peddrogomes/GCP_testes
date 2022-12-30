import base64
#import functions_framework

from google.cloud import storage

import json
import requests
import pandas as pd
import datetime
from io import StringIO

# trigger do pub/sub: cripto-ticker-extract

# Triggered from a message on a Cloud Pub/Sub topic.
#@functions_framework.cloud_event
# def hello_pubsub(cloud_event):
#   # Print out the data from Pub/Sub, to prove that it worked
#   print(base64.b64decode(cloud_event.data["message"]["data"]))



class conf():
    def __init__(self):
        self.bucket='cripto-ticker'
        self.destination_blob_name='extracao_cripto_{}.csv'



def requisicao():

  coin_list = ['BTC', 'ETH', 'XRP', 'LTC', 'ADA', 'XLM', 'LINK', 'DOGE']

  df2=pd.DataFrame(columns=['high', 'low', 'vol', 'last', 'buy', 'sell', 'open', 'date'])

  for coin in coin_list:
    url='https://www.mercadobitcoin.net/api/'+coin+'/ticker/'
    response = requests.get(url)
    r=json.loads(response.text)
      
    df=pd.DataFrame(r['ticker'],index=[coin])
    df=df.reset_index(drop=False).rename(columns={'index':'coin'})
    df.date = (datetime.datetime.utcfromtimestamp(int(df.date)) + datetime.timedelta(hours=-3))
      
    df2=pd.concat([df2,df], axis=0)
   
  return df2.reset_index(drop=True)


def upload_blob(bucket_name, source_file_name, destination_blob_name, fileformat='text/csv'):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)


    blob.upload_from_string(source_file_name.read(), fileformat)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )
  

def carrega_dataframe_no_bucket(df, bucket_name, destination_blob_name, fileformat='text/csv'):
  
  buffer = StringIO()
  
  df.to_csv(buffer)

  buffer.seek(0) #configura o arquivo para o inicio

  upload_blob(bucket_name, buffer, destination_blob_name, fileformat)


def main(event, context):

  dataframe = requisicao()

  data = datetime.datetime.strftime(dataframe.date[0],'%Y-%m-%d_%H_%M_%S')
  config=conf()
  carrega_dataframe_no_bucket(dataframe, config.bucket, config.destination_blob_name.format(data))
  


# if __name__ == '__main__':
#   main()     
