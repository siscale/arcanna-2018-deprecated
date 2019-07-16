from elasticsearch import Elasticsearch
import numpy as np
import pandas as pd 


class ConnectorElasticsearch():
	def __init__(self,context):
		self.context = context
		self.es_handle = Elasticsearch(context["target_endpoint"],port=context["target_port"])

	def get_results(self,query):
		return []

	def get_job_info(self, job_id):
		return self.es_handle.get(index=".arcanna-jobs",doc_type="_doc",id=job_id)
  
	def set_job_status(self, job_info, new_status):
		doc = {
			"doc":{
				"jobStatus": new_status 
			}
        	}

       		self.es_handle.update(index='.arcanna-jobs', id=job_info["_id"], body=doc)
		print("Updated job status from {0} to {1}".format(job_info["_source"]["jobStatus"],new_status))
	

	def normalize_datasets(self,job_info, data, normalize_labels=False):
		print("Normalizing data")
    		results = []
    		if normalize_labels is True:
			for x in data['records']:
				results += [{"doc_id":x["_id"],"model_result":x["_source"]["model_result"]}]
		else:
			print("normalizing eval batch")
	    		for index in data['records']:
				print("Iterating through records")
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
					if map_row["field"].find('.') != -1:
						#print("Found nested field while creating mapping")
						
						field_mapping[map_row['field'].split('.')[0]] = "_".join(map_row["newName"].split('.'))
					else:
						#print("Remapping {0} {1} ".format(map_row['field'], map_row['newName']))
						field_mapping[map_row['field'].split('.')[0]] = map_row['newName']
				        # Remap
       				rows = data['records'][index]	
				#print("Mapping ")
				#print(field_mapping)
				for row in rows:
          				new_row = {}
			        	temp_id = row['_id']
          				temp_index = row['_index']
          				temp_type = row['_type']
				        row = row['_source']

          				for k in row:
						#print("Saving field {0}".format(k))
		             			if k in field_mapping:
							#print(row[k])
							if isinstance(row[k],dict):
								#print("Found nested")
								#print(row[k])
								inner_key = row[k].keys()[0] 
								new_row[field_mapping[k]] = row[k][inner_key]
							else:
       	        						new_row[field_mapping[k]] = row[k]
		        			else:
							#print("Not in mapping : {0} ".format(k))
       			        			new_row[k] = row[k]
		
		        		new_row['_id'] = temp_id
	        	  		new_row['_index'] = temp_index
      					new_row['_type'] = temp_type
       					results += [new_row]
		return results


	def get_eval_batch(self,job_info, from_position, batch_size, shuffle = True):
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
                	body = '{"from":'+str(from_position)+',"size":'+str(batch_size)+',"_source":'+retrieve_fields+',"query": {"bool" : {"must_not" : {"match" : { "tags":"'+job_info["_id"]+'"}}}}}'

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
        	return current_data_batch

	def get_train_batch(self, job_info, from_position, batch_size, shuffle = True):
		# Get labeled / evaluated data 
		body = '{"size":'+str(batch_size)+',"from":'+str(from_position)+',"query":{ "match_all":{}}}'
		train_index = ".arcanna-job-"+job_info["_id"].lower()
		labeled_res = self.es_handle.search(index=train_index, body=body)
		data = {} 
		data["records"] = labeled_res["hits"]["hits"]

		data = self.normalize_datasets(job_info, data, normalize_labels=True)
		
		labels = pd.DataFrame.from_dict(data)
		labels = labels["model_result"] 
	
		# Get actual data 
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
			# Fetch only evaluated data in current train batch 
                	body = '{"from":'+str(from_position)+',"size":'+str(batch_size)+',"_source":'+retrieve_fields+',"query": {"bool" : {"must" : {"match" : { "tags":"'+job_info["_id"]+'"}}}}}'
                	res = self.es_handle.search(index=index["index"],body=body)

	                data['total_records'] += res['hits']['total']
	                data['records'][index["index"]] = res['hits']['hits']
        	current_data_batch = self.normalize_datasets(job_info, data)
	
		

	        # Shuffle if case
        	if shuffle is True:
                	idx = np.arange(0,len(current_data_batch))
               		np.random.shuffle(idx)
	        	for k in range(len(idx)):
                        	current_data_batch[k],current_data_batch[idx[k]] = current_data_batch[idx[k]],current_data_batch[k]

	        return current_data_batch, labels


	

    
	def next_batch(self,job_info, from_position, batch_size, shuffle = True, for_training=False):
		print("Training: {0}".format(for_training))
		print("Batch size: {0}".format(batch_size))
		print("From pos : {0}".format(from_position))
			
        	# Get job run ID

	        # Save job run ID

	        # Get batch start time

	        # Get maximum time

		if for_training is True:
			print("Getting train data batch") 
			return self.get_train_batch(job_info, from_position, batch_size)
		else:	
			print("Get eval data batch")
			return self.get_eval_batch(job_info, from_position, batch_size) 


	def push_evaluation(self, data, job_info, for_training=False):
		print(data)
		for ind,row in data.iterrows():
			if for_training is False:
				print("updating original document {0} with tag '{1}'".format(row["doc_id"],job_info["_id"]))
				################ Tag processed entries 
				doc = {
          			"script" : {
          				"source":"ctx._source.tags.add(params.new_tag)",
	          			"params":{
        	      				"new_tag":job_info["_id"] 
	             				}	
		          		}
       				}
				print(doc)
				print(row)
				self.es_handle.update(index=row['source_index'], doc_type=row['doc_type'], id=row["doc_id"], body=doc)
				# Todo:  This should be moved 
				self.es_handle.indices.refresh(index=row["source_index"])

				########################################################

				# Save results in elasticsearch
				new_doc = {
          				"source_index"  : str(row['source_index']),
          				"model_result": row['model_result'],
					"original_id": row["doc_id"],
          				"tags": []
       				}

				best_match = "class_0"

				class_index = 0
				max_match = row['model_result'][0]
       				for x in row['model_result']:
					if max_match < x:
						max_match = x
            					best_match = "class_"+str(class_index)
        	  			new_doc['class_'+str(class_index)] = x
					class_index += 1
				new_doc['best_match'] = best_match
				new_doc['original_match'] = best_match
				print(new_doc)
				target_index = '.arcanna-job-'+job_info["_id"].lower()
				print(target_index)
				print(row.to_dict())
       				res = self.es_handle.index(index=target_index, doc_type=row['doc_type'],body=new_doc)

