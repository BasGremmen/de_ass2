from flask import Flask, json, request, Response, jsonify, render_template
from google.cloud import bigquery
import os
import requests

app = Flask(__name__)
app.config["DEBUG"] = True


# upload to BigQuery
client = bigquery.Client(project="glass-sylph-325109")   # use your project id
table_ref = client.dataset("ass2").table("trades")  # use the correct dataset name and table name


@app.route('/<broker>')
def visualize(broker):
    # Perform a query.
    QUERY = (
        'SELECT * FROM `glass-sylph-325109.ass2.trades` WHERE broker = ' + broker + ' ORDER BY time_frame.end DESC LIMIT 100')  # use the correct project id, etc.
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    # Return an html file with the data in it
    return render_template('dashboard.html', historical=rows)


app.run(host='0.0.0.0', port=5000)
