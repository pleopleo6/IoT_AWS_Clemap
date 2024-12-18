# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import urllib.parse
import boto3
import hashlib
import tempfile

print('Loading function')

gg_clemap = boto3.client('greengrassv2')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type

    bucket = event['Records'][0]['s3']['bucket']['name']
    #bucket = 'clemapbucket'
    #key = 'output/lstm-training-job-1734343055-236c2a40/output/model.tar.gz'
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(f"Event triggered from : {bucket}")
    print(f"Key of the triggered event : {key}")
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        print(bucket,key)
        # Create a reusable Paginator
        paginator = s3_client.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket,
                        'Prefix': 'output'}
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(**operation_parameters)
        
        for page in page_iterator:
            #print(page['Contents'])
            for entry in page['Contents']:
                if (key != entry['Key']) :
                    response2 = s3_client.delete_object(Bucket=bucket,Key=entry['Key'])
                    print(f"Bucket key : {entry['Key']} deleted")
        # Component configuration
        component_name = 'com.example.clemapModel'
        # Get current component version
        current_version = get_current_component_version(component_name)
        new_version = increment_version(current_version)
        print(f"Incrementing version from : {current_version} -> {new_version}")

        # Calculate the digest of the new artifact
        artifact_digest = calculate_s3_file_digest(bucket, key)
        print(f"New calculated digest : {artifact_digest}")

        # Update the recipe
        updated_recipe = update_recipe_with_new_digest(component_name, new_version, artifact_digest, bucket, key)
        
        # Register the new component version
        register_new_component_version(updated_recipe)
        response = gg_clemap.create_deployment(
        targetArn = 'arn:aws:iot:us-east-1:060795903373:thinggroup/ClemapDummyGroup',
        deploymentName = 'DeploymentFromLambda',
        components = {
            'com.example.clemapModel': {
                'componentVersion': new_version
                },
            'aws.greengrass.Cli': {
                'componentVersion': '2.13.0'
                },
            'aws.greengrass.DockerApplicationManager': {
                'componentVersion': '2.0.12'
                },
            'aws.greengrass.LambdaLauncher': {
                'componentVersion': '2.0.13'
                },
            'aws.greengrass.LambdaManager': {
                'componentVersion': '2.3.4'
                },
            'aws.greengrass.LambdaRuntimes': {
                'componentVersion': '2.0.8'},
            'aws.greengrass.LogManager': {
                'componentVersion': '2.3.8'},
            'aws.greengrass.Nucleus': {
                'componentVersion': '2.13.0'},
            'aws.greengrass.ShadowManager': {
                'componentVersion': '2.3.9'},
            'aws.greengrass.StreamManager': {
                'componentVersion': '2.1.13'},
            'aws.greengrass.TokenExchangeService': {
                'componentVersion': '2.0.3'},
            'aws.greengrass.clientdevices.IPDetector': {
                'componentVersion': '2.2.0'},
            'com.example.HelloWorld': {
                'componentVersion': '1.0.22'}
            },
            iotJobConfiguration = {},
            deploymentPolicies = {
                'failureHandlingPolicy': 'ROLLBACK',
                'componentUpdatePolicy': {'timeoutInSeconds': 60, 'action': 'NOTIFY_COMPONENTS'
                }
            }
        )
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e


def get_current_component_version(component_name):
    """Fetches the current version of the component from the Greengrass catalog."""
    try:
        response = gg_clemap.list_components()
        for component in response['components']:
            if component['componentName'] == component_name:
                print(component['latestVersion']['componentVersion'])
                return component['latestVersion']['componentVersion']
    except Exception as e:
        print(f"Error fetching component version: {e}")
        return '0.0.0'

def increment_version(version):
    """Increments a semantic version string (e.g., 1.0.3 -> 1.0.4)."""
    major, minor, patch = map(int, version.split('.'))
    return f"{major}.{minor}.{patch + 1}"


def calculate_s3_file_digest(bucket_name, key):
    """Calculates the SHA-256 digest of an S3 object."""
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    file_data = response['Body'].read()
    return hashlib.sha256(file_data).hexdigest()


def update_recipe_with_new_digest(component_name, new_version, artifact_digest, bucket_name, artifact_key):
    """Creates an updated recipe with the new digest and artifact version."""
    recipe = {
        "RecipeFormatVersion": "2020-01-25",
        "ComponentName": component_name,
        "ComponentVersion": new_version,
        "ComponentType": "aws.greengrass.generic",
        "ComponentDescription": "Updated component with new artifact",
        "Manifests": [
            {
                "Platform": {"os": "linux"},
                "Name": "Linux",
                "Lifecycle": {
                    "run": 
                    {
                        "script" : "tar --overwrite -xvzf {artifacts:path}/model.tar.gz -C /tmp/",
                        "RequiresPrivilege" : "true"
                    }
                },
                "Artifacts": [
                    {
                        "Uri": f"s3://{bucket_name}/{artifact_key}",
                        "Digest": artifact_digest,
                        "Algorithm": "SHA-256",
                        "Unarchive": "NONE",
                        "Permission": {"Read": "OWNER", "Execute": "ALL"}
                    }
                ]
            }
        ]
    }
    return recipe


def register_new_component_version(recipe):
    """Registers the new component version in the Greengrass catalog."""
    try:
        gg_clemap.create_component_version(inlineRecipe=json.dumps(recipe))
        print("New component version registered successfully.")
    except Exception as e:
        print(f"Error creating component version: {e}")
    """Registers the new component version in the Greengrass catalog."""