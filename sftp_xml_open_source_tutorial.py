# import required packages
import paramiko
import ftplib

# create ssh client 
ssh_client = paramiko.SSHClient()
 
# fill required credential information
HOSTNAME = "xfer.example.com"
#PORT = specify your port number here
USERNAME = "exampleuser"
PASSWORD = "examplepassword"

# establish connection with targeted server
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(HOSTNAME,PORT,USERNAME,PASSWORD)

print('connection established successfully')

# open a SSH session on the SFTP server
sftp = ssh_client.open_sftp()

# get list of files
files = sftp.listdir()
print(files)

# inspect a folder's contents
folder_directory = sftp.listdir('folder_name')
print(folder_directory)

# determine first file in directory
first_file =  f"{folder_directory[0]}"
print(first_file)

# store the path of the first file in directory
first_file_path = f"folder_name/{folder_directory[0]}"
print(first_file_path)

# import google cloud storage dependencies
from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import os

# declare GCP credentials
credentials_dict = {
"type": "service_account",
"project_id": "example-project",
"private_key_id": "3115e88ee1096dedc3b820156fa335ee0c8d87da",
"private_key": "-----BEGIN PRIVATE KEY-----\nyourprivekeyhere\n-----END PRIVATE KEY-----\n",
"client_email": "example-service-account@example-project.iam.gserviceaccount.com",
"client_id": "yourclientidhere",
"auth_uri": "https://accounts.google.com/o/oauth2/auth",
"token_uri": "https://oauth2.googleapis.com/token",
"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/example-service-account%40example-project.iam.gserviceaccount.com"
}

# pass JSON GCP credentials to OAuth2 package
credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    credentials_dict
)

# pass GCP credentials to gcloud storage 
client = storage.Client(credentials=credentials, project='example-project')

# create bucket variable
bucket = client.get_bucket('example-project-bucket')

# create blob variable
blob = bucket.blob(first_file, chunk_size=262144)

# upload to GCP bucket
with sftp.open(first_file_path, bufsize=32768) as f:
    blob.upload_from_file(f)

# pull XML as string from GCP bucket
import pandas as pd
import xml.etree.ElementTree as ET
blob = blob.download_as_string()
blob = blob.decode('utf-8')

# define XML parsing function
def xml2df(x):
  root = ET.XML(x)
  all_records = []
  for i, child in enumerate(root):
    record = {}
    for subchild in child:
      record[subchild.tag] = subchild.text
      for sub_subchild in subchild:
        record[sub_subchild.tag] = sub_subchild.text
        for sub_sub_subchild in sub_subchild:
          record[sub_sub_subchild.tag] = sub_sub_subchild.text
    all_records.append(record)
  return pd.DataFrame(all_records)

# create pandas dataframe from extracted nested XML
with sftp.open(first_file_path) as o:
  df = xml2df(blob)

# drop empty columns in dataframe
df = df.drop(columns='row')
# df =df.dropna(axis=1)