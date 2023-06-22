import json


class AvroGenerator:
    def __init__(self, version, json_file="full_schema.json", avro_file="ingestdatamodel.avsc"):
        self.version = version
        self.json_file = json_file
        self.avro_file = avro_file
    
    def generate_item_dict(self, property_schema, field_property, namespace):
        #initialize temporary dictionaries
        temp_dict = {}
        items = property_schema.get("items")
        items_dict = {}
        items_dict["type"] = "record"
        
        #we need to assign names to subtypes. These are not keys that appear in the data
        items_dict["name"] = items.get("title", str(field_property)+"record")

        #Generate namespace field
        temp_dict["namespace"] = namespace
        temp_namespace = str(namespace)+"."+str(items_dict["name"])
        items_dict["namespace"] = temp_namespace
        
        temp_dict["type"] = property_schema.get("type")
        temp_dict["items"] = []

        #If there are properties, this means we have a record type object as an item and need to crawl it.
        if "properties" in items.keys():
            items_dict["fields"] = []
            self.step_through_schema(items_dict, items, items["properties"].keys(), [""], temp_namespace)

            temp_dict["items"].append(items_dict)
        
        #else we can just assign the type
        else:
            temp_dict["items"] = ['null', items.get("type")]
        
        return temp_dict

    def step_through_schema(self, avro_schema, ingest_schema, properties, required, namespace):
        for field_property in properties:
            #assign schema to variable
            property_schema = ingest_schema["properties"].get(field_property)
            
            #initialize additional variables for namespace and keys list
            property_keys = list(property_schema.keys())
            #get list of required keys
            required_property = property_schema.get("required", [""])
            temp_namespace = str(namespace)+"."+str(field_property)
            temp_dict = {}
            temp_dict["name"] = field_property
            temp_dict["type"] = []

            #make field nullable if not required
            if field_property not in required: 
                temp_dict["type"].append("null")

            #If properties is a key, this means we have a record type object and need to crawl it first
            if "properties" in property_keys:
                type_dict = {"type":"record", "name":str(field_property)+"Record"}     
                temp_dict["namespace"] = temp_namespace

                type_dict["fields"] = []
                self.step_through_schema(type_dict, property_schema, property_schema["properties"].keys(), required_property,  temp_namespace)
                temp_dict["type"].append(type_dict)
            
            #If this is an enum we need to change the structure slightly from the json schema
            elif "enum" in property_keys:
                temp_dict["type"].append({"type": "enum", "name": str(field_property)+"Enum", "symbols": property_schema.get("enum")})
            
            #items are complicated to handle
            elif "items" in property_keys:
                temp_dict["type"].append(self.generate_item_dict(property_schema, field_property,  temp_namespace))

            #We can just assign directly otherwise
            else:
                temp_dict["type"].append(property_schema.get("type"))

            #Assign descriptions to docs
            if "description" in property_keys:
                temp_dict["doc"] = property_schema.get("description")
            
            #assign comments to metadata keys
            if "$comment" in property_keys:
                temp_dict["$comment"] = property_schema.get("$comment")
            if "$comments" in property_keys:
                temp_dict["$comments"] = property_schema.get("$comments")

            avro_schema["fields"].append(temp_dict)
    
    def generate(self):
        #initialize schema dict
        avro_schema = {}
        avro_schema["$data_model_vesion"] = self.version
        avro_schema["type"] = "record"
        avro_schema["name"] = "IngestDataModel"
        avro_schema["fields"] = []

        #open the json schema
        with open(self.json_file, "r") as f:
            ingest_schema = json.load(f) 
        
        #generate namespace and required fields
        namespace = "IngestDataModel"
        required = ingest_schema.get('required')

        #pull the first level schema
        properties = list(ingest_schema.get('properties').keys())
        
        #Crawl the schema
        self.step_through_schema(avro_schema, ingest_schema, properties, required, namespace)
        
        #Save generated AVRO schema
        with open(self.avro_file, "w") as f:
            json.dump(avro_schema, f, indent=2)