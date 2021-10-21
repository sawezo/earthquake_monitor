from flask import Flask

from flask_apscheduler import APScheduler
# from flask import render_template, url_for, redirect
# from mapper import create_map


app = Flask(__name__)
scheduler = APScheduler()


global map 
map = 0


# ADD CHANGE TO 55 seconds
@scheduler.task('interval', id='update_map', seconds=1, misfire_grace_time=1000)
@app.route('/')
def homepage():
	global map 
	map += 1
	return f"""<meta http-equiv="refresh" content="1" />
			   Hello World #{map}!"""
	# recent_map = create_map(df)
    # return render_template('home.html', folium_map=recent_map._repr_html_())