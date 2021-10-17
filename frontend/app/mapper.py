import folium



def create_map(df):

    # create map
    m = folium.Map(location=(0,0), zoom_start=2)

    # adding quakes
    for i in range(len(df)):
        folium.Circle(location=[df.iloc[i]['geometry>coordinates>1'], df.iloc[i]['geometry>coordinates>0']],
                    radius=df.iloc[i]['properties>mag'] * 50000,
                    
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

    # m.save('./map.html')
    return m