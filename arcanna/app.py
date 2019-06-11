from flask import Flask, request
from flask import jsonify
from elasticsearch import Elasticsearch
from lib.tf import TFNode

# Create Flask application and load configuration 
app = Flask(__name__)
app.config.from_envvar('CONFIG_PATH')


# Load up TF 
tf_node = TFNode()


# Main entry point for application
if __name__ == '__main__':
    app.run(debug=True,host=app.config['BIND_IP'],port=app.config['BIND_PORT'])
