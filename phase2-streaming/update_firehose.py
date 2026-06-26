import boto3
import json

# This script updates our existing Firehose delivery stream to use
# our Lambda function as a data transformation step.
# Records will now be: Kinesis → Firehose → Lambda → S3

firehose = boto3.client('firehose', region_name='us-east-1')

DELIVERY_STREAM_NAME = 'kuda-ecommerce-firehose'
LAMBDA_ARN = 'arn:aws:lambda:us-east-1:755352605024:function:kuda-firehose-transformer'
S3_BUCKET = 'kuda-ecommerce-datalake'
ROLE_ARN = 'arn:aws:iam::755352605024:role/FirehoseDeliveryRole'

def update_firehose_with_lambda():
    """
    Updates the Firehose delivery stream to enable Lambda transformation.
    Firehose will now call our Lambda function for every batch of records
    before writing them to S3.
    """
    
    try:
        response = firehose.update_destination(
            DeliveryStreamName=DELIVERY_STREAM_NAME,
            
            # CurrentDeliveryStreamVersionId is required by AWS to prevent
            # accidental overwrites — it's a concurrency safety check
            CurrentDeliveryStreamVersionId='1',
            
            # We're updating the S3 destination configuration
            DestinationId='destinationId-000000000001',
            
            ExtendedS3DestinationUpdate={
                'RoleARN': ROLE_ARN,
                'BucketARN': f'arn:aws:s3:::{S3_BUCKET}',
                'Prefix': 'streaming/orders/',
                'BufferingHints': {
                    'SizeInMBs': 5,
                    'IntervalInSeconds': 60
                },
                'CompressionFormat': 'UNCOMPRESSED',
                
                # This is the new part — enable Lambda transformation
                'ProcessingConfiguration': {
                    'Enabled': True,
                    'Processors': [
                        {
                            'Type': 'Lambda',
                            'Parameters': [
                                {
                                    'ParameterName': 'LambdaArn',
                                    'ParameterValue': LAMBDA_ARN
                                }
                            ]
                        }
                    ]
                }
            }
        )
        print("Firehose updated to use Lambda transformer successfully!")
        return response
    
    except Exception as e:
        print(f"Error updating Firehose: {e}")

if __name__ == "__main__":
    print("Updating Firehose with Lambda transformation...\n")
    update_firehose_with_lambda()
    print("\nDone! Pipeline is now: Kinesis → Firehose → Lambda → S3")