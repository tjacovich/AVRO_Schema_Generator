import json
import os

class Crawler:
    def __init__(self, schema_dir, output_dir, json_file='full_schema.json', base_schema_file='Document.json'):
        self.schema_dir = schema_dir
        self.output_dir = output_dir
        self.base_schema_name = base_schema_file
        self.json_file = json_file

    def crawler(self, schema):
        for key in schema.keys():
            if isinstance(schema[key], dict):
                #If schema[key] is a dict, we first crawl that dict
                self.crawler(schema[key])
                """Once we have confirmed that we have crawled every internal dict
                    we then check to see if the dict at the current level contains a $ref key.
                """
                if schema[key].get("$ref"):
                    #We read in the schema defined by the $ref key
                    with open(schema[key].get("$ref"), "r") as f:
                        subschema = json.load(f)
                    #We check to see if this needs to be crawled further
                    self.crawler(subschema)
                    #Once that is done, we can assign the dereferenced schema to the original schema in place of {"$ref": "uri"}
                    schema[key] = subschema

    def crawl(self):
        os.chdir(self.schema_dir)
        with open(self.base_schema_name, 'r') as f:
            base_schema = json.load(f)

        self.crawler(base_schema)

        with open(str(self.json_file), "w") as f:
            json.dump(base_schema, f, indent=4)
