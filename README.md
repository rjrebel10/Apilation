# Apilation
Python Module to interact with Apilation's NoSQL warehouse API. 
Gateway documentation can be found at: http://jsonstudio.jsonar.com/gateway.html

## Functions
* pipeline_output_collection - Runs a pipeline and inserts the results into the specified target collection.
* pipeline_output_json - Runs a pipeline and returns the results as a JSON dictionary.
* insert_json_array - Inserts a JSON array into the specified target collection.

## Usage

```
Import Apilation

Data = Apilation.pipeline_output_json(username,password,server,database,pipeline,source_collection)
```
