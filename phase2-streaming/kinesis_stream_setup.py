import boto3
import json

# boto3 is the AWS SDK for Python — it lets us talk to AWS services programmatically
# We're using it here to create a Kinesis Data Stream via the AWS API

def create_kinesis_stream(stream_name, shard_count=1):
    """
    Creates a Kinesis Data Stream.
    
    stream_name: the name we give our stream
    shard_count: number of shards (think of a shard as a lane on a highway —
                 more shards = more data can flow through at once)
                 1 shard is enough for our portfolio project
    """
    
    # Connect to the Kinesis service in us-east-1 (same region as our S3 bucket)
    client = boto3.client('kinesis', region_name='us-east-1')
    
    try:
        # Send the request to AWS to create the stream
        response = client.create_stream(
            StreamName=stream_name,
            ShardCount=shard_count
        )
        print(f"Stream '{stream_name}' creation initiated successfully.")
        return response
    
    except client.exceptions.ResourceInUseException:
        # This fires if the stream already exists — safe to ignore
        print(f"Stream '{stream_name}' already exists.")
    
    except Exception as e:
        # Catch any other unexpected errors
        print(f"Error creating stream: {e}")

if __name__ == "__main__":
    # This block only runs when we execute this file directly
    # It won't run if this file is imported by another script
    create_kinesis_stream("kuda-ecommerce-stream")