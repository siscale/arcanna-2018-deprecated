from flask import Flask, request, abort
from flask import jsonify
from lib.tf import TFNode
from lib import utils 
from utils import constants
from connectors.connector_elasticsearch import ConnectorElasticsearch 
import json 
import os 
import time 

tf_node = TFNode()


# Create Flask application and load configuration 
app = Flask(__name__)
app.config.from_envvar('CONFIG_PATH')


# Load up TF 

context_data = {} 
context_data["tf_cluster_spec"] = json.loads(app.config["TF_CLUSTER_SPEC"])

context_data["node_type"] = "worker"
context_data["task_id"] = 0
context_data["target_endpoint"] = app.config["ES_ENDPOINT"]
context_data["target_port"] = 9200 

context_data["connector_handle"] = ConnectorElasticsearch(context_data) 

tf_node.set_context(context_data)
tf_node.start_node()


@app.route('/health', methods=['POST'])
def health_check():
	data = request.get_json()
	if utils.validate_request(app, data, []) is False:
		return abort(400)

        response = {
                "status":"ok"
        }
        return jsonify(response)


# POST Params:
#   - jobId
#   - action
#   - apiToken 
@app.route("/api/v1/execute", methods=['POST'])
def execute_command():
	data = request.get_json()	
	print(data)
	if utils.validate_request(app, data, ["jobId","action"]) is False:
		print("Fail in request validation")
		return abort(400)

	job_id = data['jobId']
	action = data['action']
	token = data['token']

        if utils.validate_action(action, constants.ACTIONS) is False:
		print("Fail in action validation")
		return abort(400)

	tf_node.execute(job_id,action)
	
	response = {
		"status":"ok"
	}
	return jsonify(response)

@app.route("/test/cluster", methods=["GET"])
def test_cluster():
	job_id = 1 
	action = "TRAIN"
	tf_node.execute(job_id,action)
	

# Main entry point for application
if __name__ == '__main__':
	# Make sure the token variable is set in the config
	if "API_TOKEN" not in app.config.keys() or app.config["API_TOKEN"] == "":
		print(" - you need to set API_TOKEN entry in the configuration")
		sys.exit(0)
	app.run(debug=app.config['DEBUG_MODE'],host=app.config['BIND_IP'],port=int(app.config['BIND_PORT']), use_reloader=False)
	
