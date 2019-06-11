import tensorflow as tf

class TFNode():
        def __init__(self):
                self.cluster_spec = None
                self.node_type = None
                self.task_id   = None
                self.connector = None
                self.context_data = None
                self.cluster_handle = None
                self.server  = None

        def set_context(self, context_data):
                self.cluster_spec = context_data['cluster_spec']
                self.node_type = context_data['node_type']
                self.task_id   = context_data['task_id']
                self.connector = context_data['connector_handle']
                self.context_data = context_data


        def start_node(self):
		pass 


