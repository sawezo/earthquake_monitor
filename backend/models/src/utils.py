from numpy import array, insert
from keras.preprocessing.sequence import TimeseriesGenerator


def get_y(df):
    y = df["geohash_idx"].to_numpy() # predicting the location
    y = y.reshape((-1, 1))
    return y
    
def get_param_combos(y):
    """
    need to choose batch size so that n/batch_size remainder = memory (timesteps/events looking back)
    want larger batch size for model performance so wrote wrapper func to help find
    """
    options = list()
    for train_proportion in list(range(70, 90, 1)):
        train_proportion /= 100
        split_idx = int(train_proportion*len(y))
        train = y[:split_idx]
        test = y[split_idx:]


        for memory in list(range(1, 10)):
            def print_factors(data_ct, memory):
                return {batch_size for batch_size in range(1, data_ct+1) 
                            if data_ct % batch_size == memory}

            train_batch_sizes = print_factors(len(train), memory)
            test_batch_sizes = print_factors(len(test), memory)


            common_sizes = train_batch_sizes.intersection(test_batch_sizes)
            if len(common_sizes)>=1:
                options.append((train_proportion, memory, common_sizes))
                
    return options

def tt_split(y, train_proportion, memory, batch_size):
    split_idx = int(train_proportion*len(y))
    train = y[:split_idx]
    test = y[split_idx:]

    train_generator = TimeseriesGenerator(train, train, length=memory, batch_size=batch_size)
    test_generator = TimeseriesGenerator(test, test, length=memory, batch_size=batch_size)
    return train_generator, test_generator, train.shape[1] 


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