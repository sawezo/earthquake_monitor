import os

# polish
from colorama import Fore, Style

# web
from flask import Flask, render_template, url_for, redirect

# process
from flask_apscheduler import APScheduler

# data science
import pandas as pd

# project
from utils import frame_data
from mapper import create_map
from consumer import Consumer


app = Flask(__name__)
scheduler = APScheduler()


global recent_map


# CHANGE BACK TO 60
@scheduler.task('interval', id='do_update_data', seconds=15, misfire_grace_time=1000)
@app.route("/")
def update_data():
    global recent_map

    topic, group = "quake", "quake"

    KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
    consumer_ = Consumer(topic, group, KAFKA_BROKER_URL)
    consumer_.read_messages()
    data = consumer_.message_stack
    consumer_.close()
    
    # if there is data, render it
    if len(data) > 0:
        df = frame_data(data) 
        recent_map = create_map(df)
        # ADD NEW item tracker
        return render_template('home.html', folium_map=recent_map._repr_html_())    
    
    else: # render the last 
        return render_template('home.html', folium_map=recent_map._repr_html_())    


@app.before_first_request
def initialize():
    update_data()


if __name__ == "__main__":
    scheduler.api_enabled = True
    scheduler.init_app(app)
    scheduler.start()

    client = app.test_client()

    app.run(debug=True, host='0.0.0.0', port=5000)