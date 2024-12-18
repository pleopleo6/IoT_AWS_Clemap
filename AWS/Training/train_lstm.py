import argparse
import os
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Input

def prepare_data(file_path):
    """Load and prepare data for training."""
    df = pd.read_csv(file_path)
    data = df['target'].values

    # Create sequences for 10 timesteps to predict the next value
    timesteps = 10
    X, y = [], []
    for i in range(len(data) - timesteps):
        X.append(data[i:i + timesteps])  # 10 previous values
        y.append(data[i + timesteps])   # Next value (target)
    
    # Convert to numpy arrays and reshape for LSTM input
    X = np.array(X).reshape((-1, timesteps, 1))
    y = np.array(y).reshape((-1, 1))
    print(f"Input shape: {X.shape}, Target shape: {y.shape}")
    return X, y

def create_model(timesteps):
    """Create and compile the LSTM model."""
    model = Sequential()
    model.add(Input(shape=(timesteps, 1)))
    model.add(LSTM(32, activation='relu'))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse', metrics=['accuracy'])
    return model

def train_and_save_model(X, y, epochs, save_path):
    """Train the LSTM model and save it as H5."""
    model = create_model(X.shape[1])  # timesteps
    model.fit(X, y, epochs=epochs, verbose=1)
    
    # Save the model as H5
    model.save(save_path)
    print(f"Model successfully saved as H5 at: {save_path}")

if __name__ == "__main__":
    # Parse hyperparameters
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=30, help="Number of training epochs")
    args = parser.parse_args()
    
    # Load and prepare data
    train_data, train_labels = prepare_data("s3://clemapbucket/data/Clemap_train.csv")
    
    # Define path for saving the model
    h5_model_path = "/opt/ml/model/model.h5"
    
    # Train and save model
    train_and_save_model(train_data, train_labels, args.epochs, h5_model_path)