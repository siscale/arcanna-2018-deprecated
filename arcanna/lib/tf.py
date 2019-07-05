import tensorflow as tf
from collections import namedtuple 
import pandas as pd 

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

	def build_nn(self,job_info,in_count,hidden_units=10, out_classes=2):
		#tf.reset_default_graph()
		    
		inputs = tf.placeholder(tf.float32, shape=[None, in_count-1])
		outputs = tf.placeholder(tf.float32, shape=[None, 2])
    
		labels = tf.placeholder(tf.float32, shape=[None, 2])
		learning_rate = tf.placeholder(tf.float32)
		is_training = tf.Variable(True, dtype=tf.bool)
    
		initializer = tf.contrib.layers.xavier_initializer(uniform=False,seed=23)
    
		fc = tf.layers.dense(inputs, hidden_units, activation=None, kernel_initializer=initializer)
		fc = tf.layers.batch_normalization(fc, training=is_training)
		fc = tf.nn.relu(fc)
    
		logits = tf.layers.dense(fc, 2, activation=None)
		cross_entropy = tf.nn.sigmoid_cross_entropy_with_logits(labels=labels, logits=logits)
		cost = tf.reduce_mean(cross_entropy)
    	
		with tf.control_dependencies(tf.get_collection(tf.GraphKeys.UPDATE_OPS)):
        		optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate).minimize(cost)
    
		predicted = tf.nn.sigmoid(logits)
		correct_pred = tf.equal(tf.round(predicted),labels)
		accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32))
    
		export_nodes = ['inputs', 'labels', 'learning_rate','is_training', 'logits','cost', 'optimizer', 'predicted', 'accuracy']
    
		Graph = namedtuple('Graph', export_nodes)
		local_dict = locals()
		graph = Graph(*[local_dict[each] for each in export_nodes])
    
		return graph

	def execute(self,job_id, action):
		curent_model = None 
		if self.server is None:
			print("Issue on server init. Please check logs for remediation")
		if self.node_type == "ps":
			self.server.join()
		elif self.node_type == "worker":
			# Load job info 
			job_info = self.connector.get_job_info(job_id)
			#print(job_info)
			in_count = len(job_info['_source']['indexData'][0]['fields'])
			out_count = 2 
			hidden_count = (in_count - out_count)/2 + 1

			# Load model or create a new one 
			with tf.device(tf.train.replica_device_setter(worker_device='/job:worker/task:'+str(self.task_id), cluster=self.cluster_handle)) as tf_device:
				print("Model time")
				#graph = self.build_nn(job_info, in_count, hidden_count, out_count)
				
				with tf.Session(self.server.target) as tf_sess:
					try:
						saver = tf.train.Saver()
							
						saver.restore(tf_sess, "/opt/arcanna/models/model_"+str(job_id)+".ckpt")
					except Exception as e:
						print("Failed to restore model. Check if it exists")
						print(e)
						# No model available 
						current_model = self.build_nn(job_info, in_count, hidden_count, out_count)		
				# Train time ? 
			if action == "TRAIN":
				# Todo: Set status on job 
				with tf.Session(self.server.target) as tf_sess:
					print("Train time")
					tf_sess.run(tf.global_variables_initializer())
					saver = tf.train.Saver()
    					iteration = 0
    					for e in range(epochs):
        					iteration +=1
        					for batch_x, batch_y in get_batch(train_x, train_y, batch_size): 
							feed = {
 				                	model.inputs: train_x, 
				                	model.labels: train_y,
                					model.learning_rate: learning_rate,
                					model.is_training: True
				            		}
				                	train_loss, _, train_acc = tf_sess.run([model.cost, model.optimizer, model.accuracy], feed_dict=feed)

					                if iteration % train_collect == 0:
					                	x_collect.append(e)
                						train_loss_collect.append(train_loss)
						                train_acc_collect.append(train_acc)
            
						        if iteration % train_print == 0:
					        	        print("Epoch: {}/{}".format(e+1, epochs),
                     							"Train loss: {:.4f}".format(train_loss),
					                        	"Train accuracy: {:.4f}".format(train_acc))
                    
			elif action == "EVALUATE":
				# Todo: Set status on job 
				with tf.Session(self.server.target) as tf_sess:
					tf.initialize_all_variables().run()
					print("Process time") 
					print(current_model)

					
					data = self.connector.next_batch(job_info,64) 					
					df = pd.DataFrame.from_dict(data)

					drop_columns = ['_id','_index','_type']
				        df = df.drop(drop_columns,axis=1)

				        self.input_count = len(df.columns)

					
				        # Generate the result column as random values from our class
				        for col_name in df.columns:
				        	try:
          						if df[col_name].dtype == 'object':
					             		df[col_name]= df[col_name].astype('category')
             					     		df[col_name] = df[col_name].cat.codes
       						except Exception as e:
				        		print("{0}".format(e))

					feed = {
						current_model.inputs: df,
						current_model.is_training: False 
					}

					print("Data:")
					print(df)
					print("Feed:")
					print(feed)
					result = tf_sess.run(current_model.predicted, feed)
					print(result)
	
			# Todo: Set end status for job 

