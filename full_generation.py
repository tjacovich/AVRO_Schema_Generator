import generate_avro
import schema_crawler
import os
import subprocess
import requests

#Name of the ingest data model repo
repo_owner = "adsabs"
repo_name = "ingest_data_model"

#paths for the various files
path = os.path.dirname(__file__)
model_dir = os.path.join(path, repo_name)
schema_dir = os.path.join(model_dir,"adsingestschema")
json_file = os.path.join(path,"full_schema.json")
avro_file = os.path.join(path, "ingestdatamodel.avsc")

#git clone url
data_model_url = f"https://github.com/{repo_owner}/{repo_name}.git"

#get the latest version
response = requests.get(f"https://api.github.com/repos/{repo_owner}/{repo_name}/releases/latest")
version = response.json()["tag_name"]

"""Check if repo is already local. 
Pull and checkout latest version if yes. 
Clone latest release if not.
"""
if os.path.exists(schema_dir):
    os.chdir(model_dir)
    ret_val = subprocess.run(["git", "pull", "origin", "main"], capture_output=True)
    if ret_val.returncode == 0:
        ret_val = subprocess.run(["git", "checkout", version], capture_output=True)
else:
    ret_val = subprocess.run(["git", "clone", "--branch", version, data_model_url], capture_output=True)

if ret_val.returncode == 0:
    #initialize json schema crawler
    crawler = schema_crawler.Crawler(schema_dir=schema_dir, output_dir=path)
    #initialize avro schema generator
    generator = generate_avro.AvroGenerator(version=version, json_file=json_file, avro_file=avro_file)
    #crawl the json schema and save it to file
    crawler.crawl()
    #generate the avro schema and csave it to file
    generator.generate()
else:
    print(f"git repo action: {ret_val.args}")