import boto3
import json

# boto3 client for Glue
glue = boto3.client('glue', region_name='us-east-1')

CRAWLER_NAME = 'kuda-streaming-crawler'
DATABASE_NAME = 'kuda_ecommerce_db'  # Same database we created in Phase 1
S3_TARGET = 's3://kuda-ecommerce-datalake/streaming/orders/'
ROLE_NAME = 'AWSGlueServiceRole-kuda-glue-role'

def create_streaming_crawler():
    """
    Creates a Glue crawler that scans the streaming/orders/ S3 prefix
    and catalogs the data into our existing kuda_ecommerce_db database.
    
    Once cataloged, we can query the streaming data with Athena
    just like we query the batch data from Phase 1.
    """
    
    try:
        response = glue.create_crawler(
            Name=CRAWLER_NAME,
            Role=ROLE_NAME,
            DatabaseName=DATABASE_NAME,
            Description='Crawls streaming orders data landed by Firehose',
            
            # Tell the crawler where to look in S3
            Targets={
                'S3Targets': [
                    {
                        'Path': S3_TARGET
                    }
                ]
            },
            
            # Only crawl new folders — more efficient than rescanning everything
            RecrawlPolicy={
                'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
            },
            
            # Update the table schema if it changes
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            }
        )
        print(f"Glue crawler '{CRAWLER_NAME}' created successfully.")
        return response
    
    except glue.exceptions.AlreadyExistsException:
        print(f"Crawler '{CRAWLER_NAME}' already exists.")

if __name__ == "__main__":
    print("Creating Glue streaming crawler...\n")
    create_streaming_crawler()
    print("\nCrawler ready — Step Functions will trigger it automatically!")