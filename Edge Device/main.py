"""
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


import asyncio
import logging
import time
import sqlite3
import numpy as np
import os
import time
from tensorflow.keras.models import load_model
from stream_manager import (
    ExportDefinition,
    MessageStreamDefinition,
    ReadMessagesOptions,
    ResourceNotFoundException,
    S3ExportTaskDefinition,
    S3ExportTaskExecutorConfig,
    Status,
    StatusConfig,
    StatusLevel,
    StatusMessage,
    StrategyOnFull,
    StreamManagerClient,
    StreamManagerException,
)
from stream_manager.util import Util
from keras.losses import MeanSquaredError

# Azy on regarde si Ã§a marche
# Chemin vers la base de donnÃƒÂ©es
db_path = "/home/sens/sens_docker/clemap_db/data_gathering.db"

# Chemin vers le modÃƒÂ¨le TensorFlow (format .h5)
model_path = "/tmp/model.h5"
# Fonction pour lire les donnÃƒÂ©es de la base SQLite
def read_data():
    print("Database exists:", os.path.exists(db_path))
    print("Database permissions:", oct(os.stat(db_path).st_mode))
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = """
    SELECT time, l1_p, l2_p, l3_p
    FROM meter_data
    ORDER BY time DESC
    LIMIT 10
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    # Somme des puissances des trois phases
    data = [row[1] + row[2] + row[3] for row in rows]  # l1_p + l2_p + l3_p
    return data

def read_next_val():
    print("Database exists:", os.path.exists(db_path))
    print("Database permissions:", oct(os.stat(db_path).st_mode))
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = """
    SELECT time, l1_p, l2_p, l3_p
    FROM meter_data
    ORDER BY time DESC
    LIMIT 1
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    # Somme des puissances des trois phases
    data = [row[1] + row[2] + row[3] for row in rows]  # l1_p + l2_p + l3_p
    return data

# PrÃƒÂ©paration des donnÃƒÂ©es pour le modÃƒÂ¨le
def prepare_data(data):
    # RÃƒÂ©ordonner les donnÃƒÂ©es dans l'ordre chronologique
    data = data[::-1]
    # Reshape pour que le modÃƒÂ¨le puisse lire (batch_size, time_steps, features)
    data = np.array(data, dtype=np.float32).reshape((1, len(data), 1))
    return data

def download_model():
    if not os.path.exists(model_path):
        print(f"Le fichier '{model_path}' est introuvable. Assurez-vous que le modÃƒÂ¨le TensorFlow existe.")
        return

    print(f"Chargement du modele TensorFlow depuis {model_path}...")
    # Define the custom object mapping
    custom_objects = {'mse': MeanSquaredError()}
    model = load_model(model_path,custom_objects=custom_objects,compile=False)
    return model

def predict(input_data,model):
    # Effectuer une prÃƒÂ©diction
    prediction = model.predict(input_data)
    print(f"Prediction de la prochaine puissance (sum_p) : {prediction[0][0]}")
    return prediction

def create_error_report (db_path) :
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Initialize a list to store the sums
    sums = []
    
    # Repeat the sum calculation 100 times
    for _ in range(100):
        query = """
        SELECT time, l1_p, l2_p, l3_p
        FROM meter_data
        ORDER BY time DESC
        LIMIT 10
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Sum the powers of the three phases
        data = [row[1] + row[2] + row[3] for row in rows]  # l1_p + l2_p + l3_p
        
        # Append the sum of the data (sum of all rows' sums)
        sums.append(sum(data))
    
    # Close the database connection
    conn.close()
    
    # Convert the sums list to a numpy array
    sums_array = np.array(sums)
    
    # Save the sums to a CSV file with one column named 'target'
    np.savetxt('/tmp/Clemap_train.csv', sums_array, delimiter=',', header='target', comments='', fmt='%f')

logging.basicConfig(level=logging.INFO)

# Fonction principale pour les prÃƒÂ©dictions
def predict_next_value():
    model = download_model()

    while True:
        data = read_data()

        # VÃƒÂ©rifier qu'il y a assez de donnÃƒÂ©es pour le modÃƒÂ¨le
        if len(data) < 10:
            print("Pas assez de donnÃƒÂ©es pour prÃƒÂ©dire. Attente de nouvelles donnÃƒÂ©es...")
            time.sleep(5)
            continue

        # PrÃƒÂ©parer les donnÃƒÂ©es et exÃƒÂ©cuter une prÃƒÂ©diction
        input_data = prepare_data(data)
        predict_value = predict(input_data,model)
        time.sleep(60)
        next_value = read_next_val()
        print(f"Predicted value : {predict_value}, Real value : {next_value}")
        if np.isnan(predict_value) or (abs(predict_value-next_value)>0.05):
            print("Trop de valeurs fausses, envoi de donnees pour reentrainement")
            create_error_report(db_path)
            """Check if the file has been modified."""
            past_modified_time = os.path.getmtime(model_path)
            send_data_to_cloud(logger=logging.getLogger())
            print("On attend que le nouveau model arrive")
            while True :
                current_modified_time = os.path.getmtime(model_path)
                time.sleep(5)
                if (current_modified_time!=past_modified_time) :
                    break
            print("Le nouveau ML est arrivé")
            model = download_model()
        # Attendre avant la prochaine prÃƒÂ©diction
        time.sleep(60)

# This example creates a local stream named "SomeStream", and a status stream named "SomeStatusStream.
# It adds 1 S3 Export task into the "SomeStream" stream and then stream manager automatically exports
# the data to a customer-created S3 bucket named "SomeBucket".
# This example runs until the customer-created file at URL "SomeURL" has been uploaded to the S3 bucket.


def send_data_to_cloud(logger):
    try:
        stream_name = "SomeStream"
        status_stream_name = "SomeStatusStreamName"
        bucket_name = "clemapbucket"
        key_name = "data/Clemap_train.csv"
        file_url = "file:/tmp/Clemap_train.csv"
        client = StreamManagerClient()

        # Try deleting the status stream (if it exists) so that we have a fresh start
        try:
            client.delete_message_stream(stream_name=status_stream_name)
        except ResourceNotFoundException:
            pass

        # Try deleting the stream (if it exists) so that we have a fresh start
        try:
            client.delete_message_stream(stream_name=stream_name)
        except ResourceNotFoundException:
            pass

        exports = ExportDefinition(
            s3_task_executor=[
                S3ExportTaskExecutorConfig(
                    identifier="S3TaskExecutor" + stream_name,  # Required
                    # Optional. Add an export status stream to add statuses for all S3 upload tasks.
                    status_config=StatusConfig(
                        status_level=StatusLevel.INFO,  # Default is INFO level statuses.
                        # Status Stream should be created before specifying in S3 Export Config.
                        status_stream_name=status_stream_name,
                    ),
                )
            ]
        )

        # Create the Status Stream.
        client.create_message_stream(
            MessageStreamDefinition(name=status_stream_name, strategy_on_full=StrategyOnFull.OverwriteOldestData)
        )

        # Create the message stream with the S3 Export definition.
        client.create_message_stream(
            MessageStreamDefinition(
                name=stream_name, strategy_on_full=StrategyOnFull.OverwriteOldestData, export_definition=exports
            )
        )

        # Append a S3 Task definition and print the sequence number
        s3_export_task_definition = S3ExportTaskDefinition(input_url=file_url, bucket=bucket_name, key=key_name)
        logger.info(
            "Successfully appended S3 Task Definition to stream with sequence number %d",
            client.append_message(stream_name, Util.validate_and_serialize_to_json_bytes(s3_export_task_definition)),
        )

        # Read the statuses from the export status stream
        stop_checking = False
        next_seq = 0
        while not stop_checking:
            try:
                messages_list = client.read_messages(
                    status_stream_name,
                    ReadMessagesOptions(
                        desired_start_sequence_number=next_seq, min_message_count=1, read_timeout_millis=1000
                    ),
                )
                for message in messages_list:
                    # Deserialize the status message first.
                    status_message = Util.deserialize_json_bytes_to_obj(message.payload, StatusMessage)

                    # Check the status of the status message. If the status is "Success",
                    # the file was successfully uploaded to S3.
                    # If the status was either "Failure" or "Cancelled", the server was unable to upload the file to S3.
                    # We will print the message for why the upload to S3 failed from the status message.
                    # If the status was "InProgress", the status indicates that the server has started uploading
                    # the S3 task.
                    if status_message.status == Status.Success:
                        logger.info("Successfully uploaded file at path " + file_url + " to S3.")
                        stop_checking = True
                    elif status_message.status == Status.InProgress:
                        logger.info("File upload is in Progress.")
                        next_seq = message.sequence_number + 1
                    elif status_message.status == Status.Failure or status_message.status == Status.Canceled:
                        logger.info(
                            "Unable to upload file at path " + file_url + " to S3. Message: " + status_message.message
                        )
                        stop_checking = True
                if not stop_checking:
                    time.sleep(5)
            except StreamManagerException:
                logger.exception("Exception while running")
                time.sleep(5)
    except asyncio.TimeoutError:
        logger.exception("Timed out while executing")
    except Exception:
        logger.exception("Exception while running")
    finally:
        if client:
            client.close()

# Point d'entrÃƒÂ©e principal
if __name__ == "__main__":
    # Start up this sample code
    predict_next_value()
