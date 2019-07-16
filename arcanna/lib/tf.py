import tensorflow as tf
from collections import namedtuple 
import pandas as pd 
from flask import current_app
import numpy as np


class TFNode():
        def __init__(self):
                self.cluster_spec = None
                self.node_type = None
                self.task_id   = None
                self.connector = None
                self.context_data = None
                self.cluster_handle = None
                self.server  = None
		self.server_instance = None 
		self.start_lock = False 
		self.server_started = False 

        def set_context(self, context_data):
                self.cluster_spec = context_data['tf_cluster_spec']
                self.node_type = context_data['node_type']
                self.task_id   = int(context_data['task_id'])
                self.connector = context_data['connector_handle']
                self.context_data = context_data


        def start_node(self):
		if self.start_lock is True:
			print("Skip cluster init") 
			pass 
		print("Starting TF")

		try:
			cluster_handle = tf.train.ClusterSpec(self.cluster_spec)
        	        self.server = tf.train.Server(cluster_handle, job_name=self.node_type, task_index=self.task_id)
			if self.server is not None:
				self.server_instance = self.server 
			elif self.server_instance is not None and self.server is None:
				self.server = self.server_instance 
                	self.server.start()
			self.server_started = True
			self.start_lock = True 
		except Exception as e:
			print("Failed on start_node with the following error:")
			print(e)
			return False
		return True 

	def get_model(self,job_info,in_count,hidden_units=10, out_classes=2, session=None, graph=None):
		print("Total inputs:")
		print(in_count)
		inputs = tf.placeholder(tf.float32, shape=[None, in_count], name="inputs")
		outputs = tf.placeholder(tf.float32, shape=[None, out_classes])
    
		print(inputs)

		labels = tf.placeholder(tf.float32, shape=[None, out_classes], name="labels")
		learning_rate = tf.placeholder(tf.float32, name="learning_rate")
		is_training = tf.placeholder(dtype=tf.bool, name="is_training")
    
		initializer = tf.contrib.layers.xavier_initializer()
   
		fc = tf.layers.dense(inputs, hidden_units, activation=None, kernel_initializer=initializer, name="dense_1")
		fc = tf.layers.batch_normalization(fc, training=is_training, name="normalization")
		fc = tf.nn.relu(fc, name="relu")
    
		logits = tf.layers.dense(fc, out_classes, activation=None, name="logits")
		cross_entropy = tf.nn.softmax_cross_entropy_with_logits(labels=labels, logits=logits, name="cross_entropy")
		cost = tf.reduce_mean(cross_entropy, name="cost")
   	
		with tf.variable_scope(str(in_count)):
			adam = tf.train.AdamOptimizer(learning_rate=learning_rate, name="adam")
			print(adam)

			optimizer = adam.minimize(cost, name="optimizer-"+str(in_count))
		print(optimizer)
		predicted = tf.nn.sigmoid(logits, name="predicted")
		correct_pred = tf.equal(tf.round(predicted),labels)
		accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32),name="accuracy")

		tf.global_variables_initializer().run()

		try:
			saver = tf.train.Saver(tf.all_variables(),reshape=True)
			saver.restore(session,current_app.config["MODEL_DIR"]+job_info["_id"]+"/model.ckpt")
		except Exception as e:
			print("Could not restore model. using newly initialized one.")
			print(e)
		print(inputs)
		
	def string_int(self,val):
		result = np.zeros(100, dtype=int)
		pos = 0 
		for el in val:
			result[pos] = ord(el)
			pos += 1
		return result
		
	def execute(self,job_id, action):

		if self.server is None:
			print("Issue on server init. Please check logs for remediation")
		if self.node_type == "ps":
			self.server.join()
		elif self.node_type == "worker":
			current_model = tf.Graph()
	                tf_sess = tf.Session(target=self.server.target,graph=current_model)
			tf.reset_default_graph()
			# Load job info 
			job_info = self.connector.get_job_info(job_id)
			#print(job_info)
			in_count = len(job_info['_source']['indexData'][0]['fields'])
			out_count = 2 
			hidden_count = (in_count - out_count)/2 + 1

			# Load model or create a new one 
			with tf.device(tf.train.replica_device_setter(worker_device='/job:worker/task:'+str(self.task_id), cluster=self.cluster_handle)) as tf_device:
				
				print("Model time")				
				with tf_sess:
					self.get_model(job_info, in_count, hidden_count, out_count,tf_sess,current_model)
					print(tf.trainable_variables())
					# Train time ? 
					if action == "TRAIN":
						# Todo: Set status on job 
						print("Train time")
						print(current_model)
						train_collect = 1
						train_print= 1
						epochs = 1 
						learning_rate = 0.03
  						iteration = 0
						batch_size = 64 
						x_collect, train_loss_collect, train_acc_collect = [], [], []
	    					for e in range(epochs):
							print("Epoch {0}".format(e))
							from_pos = 0 
							processed = False 
        						iteration +=1
							while processed is False:
								print("Batch process from {0} to {1}".format(from_pos,from_pos+batch_size))
        							batch_x, batch_y = self.connector.next_batch(job_info,from_pos, batch_size, for_training=True)
								df = pd.DataFrame.from_dict(batch_x)
								df.fillna(0)
       		                                        	# Keep for later on when we send back the data
		                                                orig_df = pd.DataFrame.from_dict(batch_x)

        	       		                                drop_columns = ['_id','_index','_type']
                	       		                        df = df.drop(drop_columns,axis=1)
								#for col_name in df.columns:	
								#	print(col_name)
								#	if df[col_name].dtype == 'object':
								#		df[col_name] = df[col_name].apply(lambda row: self.string_int(row) )
 
	
	
								
								if len(batch_x) == 0:
									print("Finished processing batches for epoch {0}".format(e))
									processed = True  
									continue

								#print("Inputs:")
								#print(df)
								#print("Labels:")
								#print(batch_y)
								df.fillna(0, inplace=True)
								if len(df) != len(batch_y):
									print("Skipping batch because of length diff")
									processed = True
									continue
								feed = {
		 				                	"inputs:0": np.asarray(df).tolist(), 
						                	"labels:0": np.asarray(batch_y.values).tolist(),
               								"learning_rate:0": learning_rate,
               								"is_training:0": True
			        			    	}
							
					        	       	train_loss, _, train_acc = tf_sess.run(["cost:0", "optimizer"+str(in_count), "accuracy:0"], feed_dict=feed)
							        if iteration % train_collect == 0:
							               	x_collect.append(e)
               								train_loss_collect.append(train_loss)
						                	train_acc_collect.append(train_acc)
      							        if iteration % train_print == 0:
					        		        print("Epoch: {}/{}".format(e+1, epochs),
               									"Train loss: {:.4f}".format(train_loss),
				                	        		"Train accuracy: {:.4f}".format(train_acc))
								from_pos += batch_size 
						
						#with tf.device(tf.train.replica_device_setter(worker_device='/job:wo$
			                        print("Saving model for '{0}'".format(job_info["_id"]))

						#                                with tf_sess:
	                                        try:
        	                                        saver = tf.train.Saver(reshape=True)

                	                                saver.save(tf_sess, current_app.config["MODEL_DIR"]+job_info["_id"]+"/model.ckpt")

                      	                  	except Exception as e:
                                                	print("Failed to save model. Check if there are enough permissions")

	                                                print(e)
                    
					elif action == "EVALUATE":
						# Todo: Set status on job 
						self.connector.set_job_status(job_info,"EVALUATING")
						tf_sess.run(tf.global_variables_initializer())
						print("Process time") 
						#print(current_model)
						
						from_pos = 0 
						if "batch_pos" in job_info.keys():
							from_pos = job_info["batch_pos"] 
						batch_size = 64 
						processed = False
						while processed is False:
							data = self.connector.next_batch(job_info, from_pos, batch_size) 		
							print("Data count: {0}".format(len(data)))
							if len(data) == 0:
								print("Finished processing") 
								processed = True
								continue 
							df = pd.DataFrame.from_dict(data) 
							df.fillna(0)
							# Keep for later on when we send back the data 
							orig_df = pd.DataFrame.from_dict(data)
	
							drop_columns = ['_id','_index','_type']
					        	df = df.drop(drop_columns,axis=1)

	
							#print(df)
						        # Generate the result column as random values from our class
						        #for col_name in df.columns:
          						#	if df[col_name].dtype == 'object':
							#		df[col_name] = df[col_name].apply(lambda row: self.string_int(row))

							feed = {
								"inputs:0": df,
								"is_training:0": False 
							}

							result = tf_sess.run("predicted:0", feed)
							result = np.nan_to_num(result,True)

							df["model_result"] = result.tolist() 
							df["source_index"] = orig_df["_index"] 
							df["doc_id"] = orig_df["_id"] 	
							df["doc_type"] = orig_df["_type"]	
							# Save results back in Elasticsearch 
							self.connector.push_evaluation(df, job_info, for_training=False)
		
							from_pos += batch_size 
						# Set job status to idle
                                	        self.connector.set_job_status(job_info, "IDLE")

