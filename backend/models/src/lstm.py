import numpy as np

from keras.models import Sequential
from keras.optimizers import Adam
from keras.layers.core import Dense, Dropout
from keras.metrics import top_k_categorical_accuracy
from keras.layers.recurrent import LSTM as LSTM_LAYER


class LSTM:
    def __init__(self, DROPOUT, ACTIVATION, ALPHA, UNITS, CLASS_COUNT, BATCH_SIZE, MEMORY, TRAINING_WIDTH, ACCURACY_K):
        self.DROPOUT = DROPOUT
        self.ACTIVATION = ACTIVATION
        self.ALPHA = ALPHA
        self.UNITS = UNITS
        self.CLASS_COUNT = CLASS_COUNT
        self.BATCH_SIZE = BATCH_SIZE
        self.MEMORY = MEMORY
        self.TRAINING_WIDTH = TRAINING_WIDTH
        self.ACCURACY_K = ACCURACY_K
        
        self.batchsize__mem__input_dim = (self.BATCH_SIZE, self.MEMORY, self.TRAINING_WIDTH) 

        self.model = self.structure()

    def top_k_metric(self, y_true, y_pred):
        """
        accuracy metric tracks how many predictions are in the top K 
        """
        return top_k_categorical_accuracy(y_true, y_pred, k=self.ACCURACY_K)

    def structure(self):
        """
        define model structure

        did not 1-hot encode since using sparse loss
        """
        model = Sequential()

        model.add(LSTM_LAYER(self.UNITS, 
                                name="input_lstm",
                                activation=self.ACTIVATION, 
                                batch_input_shape=self.batchsize__mem__input_dim, 
                                return_sequences=True, 
                                stateful=True, 
                                recurrent_dropout=self.DROPOUT)) 

        model.add(LSTM_LAYER(self.UNITS, 
                                name="hidden_lstm",
                                activation=self.ACTIVATION,
                                recurrent_dropout=self.DROPOUT,
                                return_sequences=False))

        model.add(Dropout(self.DROPOUT, name="dropout"))

        model.add(Dense(self.CLASS_COUNT, 
                        name="output",
                        activation="softmax")) 

        
        model.compile(loss="sparse_categorical_crossentropy", 
                      optimizer=Adam(learning_rate=self.ALPHA), 
                      sample_weight_mode="temporal",
                      metrics=[self.top_k_metric, "sparse_categorical_accuracy"])
        
        print(model.summary())

        return model

    def stateful_train(self, train_generator, EPOCHS):
        for i in range(EPOCHS):
            print()
    
            self.model.fit_generator(train_generator, epochs=1, shuffle=False)
            self.model.reset_states()

    def test(self, test_generator):
        # trimming slightly to fit batch size
        prediction = self.model.predict_generator(test_generator, verbose=1)
        return np.argmax(prediction, axis=1) # predicted class indices