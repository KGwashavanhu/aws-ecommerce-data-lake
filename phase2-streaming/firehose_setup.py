import boto3
import json
import time

# boto3 clients for IAM and Firehose
iam = boto3.client('iam', region_name='us-east-1')
firehose = boto3.client('firehose', region_name='us-east-1')

# The S3 bucket we created in Phase 1
S3_BUCKET = 'kuda-ecommerce-datalake'
STREAM_NAME = 'kuda-ecommerce-stream'
DELIVERY_STREAM_NAME = 'kuda-ecommerce-firehose'
ROLE_NAME = 'FirehoseDeliveryRole'

def create_firehose_role():
    """
    Creates an IAM role that Firehose will assume to write to S3.
    Every AWS service that does something on your behalf needs a role —
    this is how AWS knows Firehose is allowed to touch your S3 bucket.
    """
    
    # Trust policy — tells AWS that Firehose is allowed to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "firehose.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        # Create the role with the trust policy
        role = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Allows Firehose to deliver data to S3'
        )
        print(f"IAM role '{ROLE_NAME}' created successfully.")
        
        # Permission policy — defines what the role is actually allowed to do
        # In this case: read from Kinesis and write to S3
        permission_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:GetBucketLocation",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{S3_BUCKET}",
                        f"arn:aws:s3:::{S3_BUCKET}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "kinesis:GetRecords",
                        "kinesis:GetShardIterator",
                        "kinesis:DescribeStream",
                        "kinesis:ListStreams"
                    ],
                    "Resource": f"arn:aws:kinesis:us-east-1:755352605024:stream/{STREAM_NAME}"
                }
            ]
        }
        
        # Attach the permission policy to the role
        iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName='FirehoseS3KinesisPolicy',
            PolicyDocument=json.dumps(permission_policy)
        )
        print("Permissions attached to role.")
        
        # Wait a few seconds for IAM to propagate the role
        # (AWS needs a moment before the role can be used)
        print("Waiting for IAM role to propagate...")
        time.sleep(10)
        
        return role['Role']['Arn']
    
    except iam.exceptions.EntityAlreadyExistsException:
        # Role already exists — just fetch and return its ARN
        print(f"Role '{ROLE_NAME}' already exists, fetching ARN...")
        role = iam.get_role(RoleName=ROLE_NAME)
        return role['Role']['Arn']

def create_delivery_stream(role_arn):
    """
    Creates a Kinesis Firehose delivery stream.
    Firehose reads from our Kinesis Data Stream and buffers records,
    then delivers them to S3 in batches (every 60 seconds or 5MB).
    """
    
    try:
        response = firehose.create_delivery_stream(
            DeliveryStreamName=DELIVERY_STREAM_NAME,
            
            # Tell Firehose to read from our Kinesis Data Stream
            DeliveryStreamType='KinesisStreamAsSource',
            KinesisStreamSourceConfiguration={
                'KinesisStreamARN': f'arn:aws:kinesis:us-east-1:755352605024:stream/{STREAM_NAME}',
                'RoleARN': role_arn
            },
            
            # Tell Firehose to deliver to S3
            ExtendedS3DestinationConfiguration={
                'RoleARN': role_arn,
                'BucketARN': f'arn:aws:s3:::{S3_BUCKET}',
                
                # Store streaming data under a separate prefix so it doesn't
                # mix with our Phase 1 batch data
                'Prefix': 'streaming/orders/',
                
                # Buffer settings — deliver to S3 every 60 seconds or when 5MB accumulates
                'BufferingHints': {
                    'SizeInMBs': 5,
                    'IntervalInSeconds': 60
                },
                'CompressionFormat': 'UNCOMPRESSED'
            }
        )
        print(f"Firehose delivery stream '{DELIVERY_STREAM_NAME}' created successfully.")
        return response
    
    except firehose.exceptions.ResourceInUseException:
        print(f"Delivery stream '{DELIVERY_STREAM_NAME}' already exists.")

if __name__ == "__main__":
    print("Setting up Firehose delivery stream...\n")
    role_arn = create_firehose_role()
    create_delivery_stream(role_arn)
    print("\nFirehose setup complete!")
    print(f"Data will land in s3://{S3_BUCKET}/streaming/orders/")