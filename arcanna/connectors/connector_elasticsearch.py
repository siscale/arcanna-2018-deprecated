from elasticsearch import Elasticsearch
import numpy as np


class ConnectorElasticsearch():
   def __init__(self,context):
      self.context = context
      self.es_handle = Elasticsearch(context["target_endpoint"],port=context["target_port"])

   def get_results(self,query):
      return []

   def get_job_info(self, job_id):
	return self.es_handle.get(index=".arcanna-jobs",doc_type="_doc",id=job_id)


   def normalize_datasets(self,job_info, data):
    results = []
    for index in data['records']:
       # Get mapping
       mapping = []
       for ind in job_info["_source"]["indexData"]:
          if ind["index"] == index:
             mapping = ind["fields"]
             break
       field_mapping = {}
       for map_row in mapping:
		if "." in map_row["newName"]:
			processed = False
			temp_val = ""
			result = {} 
			if map_row["newName"].find('.') != -1:
				result = {map_row["newName"].split('.')[0]: map_row["newName"].split('.')[1]}
				field_mapping[map_row['field']] = map_row["newName"].split('.')[0]				
		else:
			field_mapping[map_row['field'].split('.')[0]] = map_row['newName']
       # Remap
       rows = data['records'][index]
       for row in rows:
          new_row = {}
          temp_id = row['_id']
          temp_index = row['_index']
          temp_type = row['_type']

          row = row['_source']

          for k in row:
             if k in field_mapping:
		print("{0} {1} {2}".format(k,row[k],isinstance(row[k],dict)))
		if isinstance(row[k],dict):
			inner_key = row[k].keys()[0] 
			new_row[field_mapping[k]] = row[k][inner_key]
		else:
                	new_row[field_mapping[k]] = row[k]
             else:
                new_row[k] = row[k]
		
          new_row['_id'] = temp_id
          new_row['_index'] = temp_index
          new_row['_type'] = temp_type
          results += [new_row]
    return results


   def next_batch(self,job_info, batch_size, shuffle = True, for_training=False):
        # Get job run ID

        # Save job run ID

        # Get batch start time

        # Get maximum time

        # Fetch data
        total_count = 0
        data = {}
        data['total_records'] = 0
        data['records'] = {}
        for index in job_info["_source"]["indexData"]:
                retrieve_fields = "["
                for x in index["fields"]:
                        retrieve_fields += "\""+x["field"]+"\","
		retrieve_fields = retrieve_fields[:-1]
                retrieve_fields += "]"
		body = '{"size":'+str(batch_size)+',"_source":'+retrieve_fields+',"query": {"bool" : {"must_not" : {"match" : { "tags":"'+job_info["_id"]+'"}}}}}'
                res = self.es_handle.search(index=index["index"],body=body)
			
                data['total_records'] += res['hits']['total']
                data['records'][index["index"]] = res['hits']['hits']
        current_data_batch = self.normalize_datasets(job_info, data)
        # Save last batch timestamp

        # Shuffle if case
        if shuffle is True:
                idx = np.arange(0,len(current_data_batch))
                np.random.shuffle(idx)

		for k in range(len(idx)):
			current_data_batch[k],current_data_batch[idx[k]] = current_data_batch[idx[k]],current_data_batch[k]
	print(current_data_batch)
        return current_data_batch

