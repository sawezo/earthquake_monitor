import pandas as pd
import geohash as gh
from datetime import datetime


def prepare_historical_data(min_magnitude, cols_to_keep):
    # data preparation
    df = pd.read_csv("../data/historical.csv")

    # cleaning bad dates in the data found in inspection
    df.loc[3378, 'Date'] = "02/23/1975"
    df.loc[3378, 'Time'] = "02:53:41"

    df.loc[7512, 'Date'] = "04/28/1985"
    df.loc[7512, 'Time'] = "02:53:41"

    df.loc[20650, 'Date'] = "03/13/2011"
    df.loc[20650, 'Time'] = "02:23:34"


    # formatting time
    df["time"] = df["Date"]+"_"+df["Time"]
    df['time'] =  pd.to_datetime(df['time'], format='%m/%d/%Y_%H:%M:%S')
    df.sort_values("time", inplace=True)
    df.set_axis(df['time'], inplace=True)
    time_range = df.time.to_numpy() # for plotting...


    # trim
    df = df[df["Magnitude"]>=min_magnitude]
    df = df[cols_to_keep]


    # tidy
    df.rename(dict(zip(cols_to_keep, ["longitude", "latitude", "magnitude"])), 
            axis=1, inplace=True)


    # geohashing
    df["geohash"] = df.apply(lambda x: gh.encode(x["latitude"], x["longitude"], precision=2), axis=1)
    df.drop(["latitude", "longitude"], axis=1, inplace=True)

    # integer encode
    df["geohash_idx"] =  pd.Categorical(df.geohash).codes
    geohash_idx2hash = dict(zip(df.geohash_idx, df.geohash))

    CLASS_COUNT = df["geohash_idx"].nunique() # is number of unique classes
    df.reset_index(drop=True, inplace=True)

    return df, geohash_idx2hash

def unpack_predictions(max_index_col, geohash_idx2hash):
    """ 
    decoding geohash predictions
    """
    lat__long = [gh.decode(geohash_idx2hash[e]) for e in max_index_col]
    prediction_df = pd.DataFrame(lat__long, columns=["latitude", "longitude"]) # same order as expected in geohash encoding

    prediction_df.drop_duplicates(inplace=True)
    prediction_df.reset_index(drop=True)

    return prediction_df