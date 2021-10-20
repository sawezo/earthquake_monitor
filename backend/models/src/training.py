from numpy import array, insert, hstack, delete, unique

from keras.models import Sequential
from keras.optimizers import SGD, Adam
from keras.layers.recurrent import LSTM
from keras.layers import TimeDistributed
from keras.metrics import top_k_categorical_accuracy
from keras.layers.core import Dense, Activation, Dropout
from keras.preprocessing.sequence import TimeseriesGenerator

# # from pathlib import Path, PosixPath
# # import numpy as np
# # import pandas as pd
# # import logging
# # from datetime import date
# # from joblib import dump


# SEED = 42
# MODEL_PATH = 'dags/ml_project/models/'

# np.random.seed(SEED)


def get_y(df):
    y = df["geohash_idx"].to_numpy() # predicting the location
    y = y.reshape((-1, 1))
    return y

def tt_split(y, train_proportion):
    split_idx = int(train_proportion*len(y))
    geo_train = y[:split_idx]
    geo_test = y[split_idx:]


# import numpy as np # linear algebra
# import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# from subprocess import check_output


# from sklearn.cross_validation import  train_test_split
# import time #helper libraries
# from sklearn.preprocessing import MinMaxScaler
# import matplotlib.pyplot as plt
# from numpy import newaxis
# from geopy.geocoders import Nominatim

# %matplotlib inline
# import warnings
# warnings.filterwarnings('ignore')
# %config InlineBackend.figure_format = 'retina'
# # Any results you write to the current directory are saved as output.

# from subprocess import check_output
# print(check_output(["ls", "../input"]).decode("utf8"))






# test.shape
# testX, testY = create_dataset(test, look_back)


# test1 = np.array(testX[0:1])
# print(test1[0:1][0:1])


# model.predict(test1)


def compile_model():
    pass
# # update model on new data only with a smaller learning rate
# # First, we must use a much smaller learning rate in an attempt to use the current weights as a starting point for the search.


# opt = SGD(learning_rate=0.001, momentum=0.9)
# # compile the model
# model.compile(optimizer=opt, loss='binary_crossentropy')


# model.compile(optimizer=opt, loss='binary_crossentropy')
# # fit the model on new data
# model.fit(X_new, y_new, epochs=100, batch_size=32, verbose=0)


# def train():
#     """
#     wrapper for model training
#     """        print("data inserted into mongo")

#     pass
#     # df, target = _get_data()
#     # model = _build_ml_pipeline(df)

#     # model.fit(df, target)
#     # _save_model(model, Path(MODEL_PATH))

# def _build_ml_pipeline(df: pd.DataFrame) -> Pipeline:
#     """
#     Builds ML pipeline using sklearn.

#     :param df: the data frame of the features.
#     :return Pipeline:
#     """
#     features_numerical = list(df.select_dtypes(include=['float64']).columns)
#     features_categorical = list(df.select_dtypes(include=['category']).columns)

#     logging.info(f'### A number of numerical features: {len(features_numerical)}')
#     logging.info(f'### A number of categorical features: {len(features_categorical)}')

#     # transformers
#     transformer_numerical = Pipeline(steps=[('scaler', StandardScaler())])
#     transformer_categorical = Pipeline(steps=[('ohe', OneHotEncoder(handle_unknown='ignore'))])

#     # processor
#     preprocessor = ColumnTransformer(transformers=[('numerical', transformer_numerical, features_numerical),
#                                                    ('categorical', transformer_categorical, features_categorical)])

#     # full pipeline
#     model = Pipeline(steps=[('preprocessor', preprocessor),
#                             ('regressor', RandomForestRegressor(n_estimators=250, random_state=SEED))])

#     return model


# def _save_model(model: Pipeline, model_path: PosixPath):
#     """
#     Saves the trained model with the sufix of the current day in the name of the file.

#     :param model:
#     :param model_path:
#     :return:
#     """
#     path = model_path.joinpath(f'model_{date.today().isoformat()}.joblib')
#     dump(model, path)