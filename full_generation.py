import generate_avro
import schema_crawler
import os
import argparse
import requests

parser = argparse.ArgumentParser(
                    prog='JSON to AVRO Schema Converter',
                    description='Converts JSON schemae into equivalent AVRO versions.')

parser.add_argument("--repo_owner", "-o",
                    action='store',
                    default='adsabs')

parser.add_argument("--repo_name", "-r",
                    action='store',
                    default='ingest_data_model')

parser.add_argument("--schema_dir", "-d",
                    action='store',
                    default='adsingestschema')

parser.add_argument("--version", "-v",
                    action='store')

args = parser.parse_args()


#Name of the ingest data model repo
repo_owner = args.repo_owner
repo_name = args.repo_name
version = args.version

#paths for the various files
path = os.path.dirname(__file__)
model_dir = str(path)+'/'+str(repo_name)
schema_dir = str(model_dir)+'/'+str(args.schema_dir)
json_file = str(path)+'/'+str(repo_name)+"_full_schema.json"
avro_file = str(path)+'/'+str(repo_name)+".avsc"

#git clone url
data_model_url = "https://github.com/{}/{}.git".format(repo_owner, repo_name)

#get the latest version
if not version:
    try:
        response = requests.get("https://api.github.com/repos/{}/{}/releases/latest".format(repo_owner, repo_name))
        version = response.json()["tag_name"]
    except:
        print("No releases. Pulling from last commit.")

"""Check if repo is already local. 
Pull and checkout latest version if yes. 
Clone latest release if not.
"""
if os.path.exists(schema_dir):
    os.chdir(model_dir)
    os.system("git pull origin main --tags")
    os.system("git checkout "+str(version))
else:
    os.system("git clone --branch "+str(version)+" "+str(data_model_url))

#initialize json schema crawler
crawler = schema_crawler.Crawler(schema_dir=schema_dir, json_file=json_file, output_dir=path)
#initialize avro schema generator
generator = generate_avro.AvroGenerator(version=version, json_file=json_file, avro_file=avro_file)

#crawl the json schema and save it to file
crawler.crawl()
#generate the avro schema and csave it to file
generator.generate()