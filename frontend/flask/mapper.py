import os
import folium
import psycopg2


POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']


def connect_to_database():
    conn = psycopg2.connect(f"dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host=postgres_db")
    return conn

def pull_recent_quakes(count):
    """
    get the most recent earthquakes from the database
    """
    # connect to the earthquake database
    conn = connect_to_database()
    cur = conn.cursor()
    
    # select the most recent earthquakes
    cur.execute("SELECT * FROM quake_sink limit {count};") # ADD base off timestamp
    recent_quakes = cur.fetchall()
    
    # cleanup
    cur.close()
    conn.close()

    # package as a dataframe
    processed_quakes = [[float(value) for value in q] for q in recent_quakes]
    quake_df = pd.DataFrame(processed_quakes, columns=["magnitude", "longitude", "latitude"])
    return quake_df

def create_map(df):
    # create map
    m = folium.Map(location=(0,0), zoom_start=2)

    # adding quakes
    for i in range(len(df)):
        folium.Circle(location=[df.iloc[i]['latitude'], df.iloc[i]['longitude']],
                    radius=df.iloc[i]['magnitude'] * 50000,
                    
                    # polish
                    weight=1,  # thickness of the border
                    color='red',  # this is the color of the border
                    opacity=0.3,  # this is the alpha for the border
                    fill_color='red',  # fill is inside the circle
                    fill_opacity=0.1,  # we will make that less opaque so we can see layers
    ).add_to(m)


    # tectonic plates
    url = 'https://raw.githubusercontent.com/fraxen/tectonicplates/master/GeoJSON/PB2002_boundaries.json'
    folium.GeoJson(
        url,
        name='geojson'
    ).add_to(m)


    folium.LayerControl().add_to(m)

    return m