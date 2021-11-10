#!flask/bin/python
from flask import Flask, jsonify
import os

app = Flask(__name__)

@app.route('/')
def index():
    return "Welcome to Dragoman Service"

@app.route('/mapping_transformation/<path:config_file>', methods=['GET','POST'])
def rdfgraph(config_file):
	os.system("python3 /app/run_translator.py /" + config_file)
	return "The file has been semantified " + str(config_file) + "\n"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=4000)