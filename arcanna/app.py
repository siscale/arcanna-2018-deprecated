from flask import Flask, request, abort
from flask import jsonify
from elasticsearch import Elasticsearch
from lib.tf import TFNode

# Create Flask application and load configuration 
app = Flask(__name__)
app.config.from_envvar('CONFIG_PATH')


# Load up TF 
tf_node = TFNode()


@app.route('/health', methods=['POST'])
def health_check():
        token = request.values.get('api_token')
        if token is None or token != current_app.config("API_TOKEN"):
                return abort(400)
        response = {
                "status":"ok"
        }
        return jsonify(response)


# Main entry point for application
if __name__ == '__main__':
	if "API_TOKEN" not in app.config.keys() or app.config["API_TOKEN"] == "":
		print(" - you need to set API_TOKEN entry in the configuration")
		sys.exit(0)

	app.run(debug=True,host=app.config['BIND_IP'],port=app.config['BIND_PORT'])
	
