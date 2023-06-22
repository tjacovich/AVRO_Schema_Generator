import generate_avro
import schema_crawler
import os
import requests

#Name of the ingest data model repo
repo_owner = "adsabs"
repo_name = "ingest_data_model"

#paths for the various files
path = os.path.dirname(__file__)
model_dir = str(path)+'/'+str(repo_name)
schema_dir = str(model_dir)+'/adsingestschema'
json_file = str(path)+"/full_schema.json"
avro_file = str(path)+"/ingestdatamodel.avsc"

#git clone url
data_model_url = "https://github.com/{}/{}.git".format(repo_owner, repo_name)

#get the latest version
response = requests.get("https://api.github.com/repos/{}/{}/releases/latest".format(repo_owner, repo_name))
version = response.json()["tag_name"]

"""Check if repo is already local. 
Pull and checkout latest version if yes. 
Clone latest release if not.
"""
if os.path.exists(schema_dir):
    os.chdir(model_dir)
    os.system("git pull origin main")
    os.system("git checkout "+str(version))
else:
    os.system("git clone --branch "+str(version)+" "+str(data_model_url))

#initialize json schema crawler
crawler = schema_crawler.Crawler(schema_dir=schema_dir, output_dir=path)
#initialize avro schema generator
generator = generate_avro.AvroGenerator(version=version, json_file=json_file, avro_file=avro_file)

#crawl the json schema and save it to file
crawler.crawl()
#generate the avro schema and csave it to file
generator.generate()