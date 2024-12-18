import json
import boto3
import os
import uuid
import time

# Client S3 et SageMaker
s3_client = boto3.client('s3')
sagemaker_client = boto3.client('sagemaker')

# Paramètres de ton job SageMaker
ROLE = 'arn:aws:iam::060795903373:role/service-role/AmazonSageMaker-ExecutionRole-20241114T100363'  # Remplace par ton rôle SageMaker
BUCKET_NAME = 'clemapbucket'
MODEL_OUTPUT_PATH = 's3://clemapbucket/output/'  # Emplacement de sortie pour les résultats du modèle
SOURCE_INPUT_PATH = 's3://clemapbucket/scripts/train_lstm.py'
TRAINING_IMAGE = '763104351884.dkr.ecr.us-east-1.amazonaws.com/tensorflow-training:2.16.2-cpu-py310'  # Image Docker pour l'entraînement
KEY = 'data/Clemap_train.csv'  # Corrected key to point to the file directly

def lambda_handler(event, context):
    # Créer un job d'entraînement SageMaker
    timestamp = str(int(time.time()))
    unique_suffix = str(uuid.uuid4())[:8]  # Limite à 8 caractères
    job_name = f"lstm-training-job-{timestamp}-{unique_suffix}"
    
    # Configurer le job d'entraînement
    response = sagemaker_client.create_training_job(
        TrainingJobName=job_name,
        AlgorithmSpecification={
            'TrainingImage': TRAINING_IMAGE,
            'TrainingInputMode': 'File',
            "ContainerEntrypoint": ["/usr/local/bin/python3.10"],
            "ContainerArguments": ["/opt/ml/input/data/scripts/train_lstm.py"]
        },
        RoleArn=ROLE,
        InputDataConfig=[
            {
                'ChannelName': 'training',
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': f's3://{BUCKET_NAME}/data',  # Use dynamic bucket and key from event
                        'S3DataDistributionType': 'FullyReplicated'
                    }
                },
                'ContentType': 'csv',
                'ChannelName': 'scripts',
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': f's3://{BUCKET_NAME}/scripts',  # Use dynamic bucket and key from event
                        'S3DataDistributionType': 'FullyReplicated'
                    }
                },
                'ContentType': 'py',
            }
        ],
        OutputDataConfig={
            'S3OutputPath': MODEL_OUTPUT_PATH
        },
        ResourceConfig={
            'InstanceType' : 'ml.m5.large',
            'InstanceCount': 1,
            'VolumeSizeInGB': 1
        },
        StoppingCondition={
            'MaxRuntimeInSeconds': 3600  # Limiter le temps d'exécution
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Job started: {job_name}')
    }
