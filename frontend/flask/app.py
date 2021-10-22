from flask import Flask

from flask_apscheduler import APScheduler
# from flask import render_template, url_for, redirect
from mapper import pull_recent_quakes, create_map


app = Flask(__name__)
scheduler = APScheduler()


# ADD CHANGE TO 55 seconds
@scheduler.task('interval', id='update_map', seconds=1, misfire_grace_time=1000)
@app.route('/')
def homepage():
	recent_quake_df = pull_recent_quakes(count=10)
	recent_map = create_map(recent_quake_df)
    return render_template('home.html', folium_map=recent_map._repr_html_())