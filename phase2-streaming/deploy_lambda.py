import boto3
import json
import zipfile
import os
import time

# boto3 clients for Lambda and IAM
lambda_client = boto3.client('lambda', region_name='us-east-1')
iam = boto3.client('iam', region_name='us-east-1')

FUNCTION_NAME = 'kuda-firehose-transformer'
ROLE_NAME = 'LambdaFirehoseRole'

def create_lambda_role():
    """
    Creates an IAM role that Lambda will assume when it runs.
    Lambda needs permission to write logs to CloudWatch so we can
    see what happened when it processed our records.
    """
    
    # Trust policy — allows Lambda service to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        role = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Allows Lambda to transform Firehose records'
        )
        print(f"IAM role '{ROLE_NAME}' created.")
        
        # Attach AWS managed policy for basic Lambda execution
        # This gives Lambda permission to write logs to CloudWatch
        iam.attach_role_policy(
            RoleName=ROLE_NAME,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
        )
        print("Basic execution policy attached.")
        
        # Wait for IAM to propagate
        print("Waiting for role to propagate...")
        time.sleep(10)
        
        return role['Role']['Arn']
    
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"Role '{ROLE_NAME}' already exists, fetching ARN...")
        role = iam.get_role(RoleName=ROLE_NAME)
        return role['Role']['Arn']

def zip_lambda_function():
    """
    Zips up our lambda_transformer.py file.
    AWS Lambda requires code to be uploaded as a zip file.
    """
    zip_path = 'lambda_transformer.zip'
    
    with zipfile.ZipFile(zip_path, 'w') as z:
        z.write('lambda_transformer.py')
    
    print(f"Lambda function zipped as '{zip_path}'.")
    return zip_path

def deploy_lambda(role_arn, zip_path):
    """
    Uploads and creates the Lambda function on AWS.
    We specify the handler as 'lambda_transformer.lambda_handler' —
    that tells AWS which file and which function to call when triggered.
    """
    
    # Read the zip file as bytes to upload
    with open(zip_path, 'rb') as f:
        zip_bytes = f.read()
    
    try:
        response = lambda_client.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime='python3.12',           # Python version to run our code
            Role=role_arn,                  # IAM role Lambda will assume
            Handler='lambda_transformer.lambda_handler',  # file.function
            Code={'ZipFile': zip_bytes},
            Timeout=60,                     # Max seconds Lambda can run
            MemorySize=128,                 # MB of memory allocated
            Description='Transforms and enriches Firehose records before S3 delivery'
        )
        print(f"Lambda function '{FUNCTION_NAME}' deployed successfully.")
        return response
    
    except lambda_client.exceptions.ResourceConflictException:
        print(f"Function '{FUNCTION_NAME}' already exists.")

if __name__ == "__main__":
    print("Deploying Lambda transformer...\n")
    role_arn = create_lambda_role()
    zip_path = zip_lambda_function()
    deploy_lambda(role_arn, zip_path)
    print("\nLambda deployment complete!")