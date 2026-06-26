import json
import base64
from datetime import datetime

# This is the Lambda function that Firehose will call for each batch of records.
# Firehose passes records in base64 encoded format, so we need to decode them,
# transform them, then re-encode them before sending back to Firehose.

def lambda_handler(event, context):
    """
    Main Lambda handler — this is the entry point AWS calls automatically.
    
    event: contains the batch of records from Firehose
    context: contains runtime info about the Lambda function (we won't use this)
    
    Must return a 'records' list with the same number of records that came in,
    each marked as 'Ok', 'Dropped', or 'ProcessingFailed'
    """
    
    output_records = []
    
    # Loop through each record in the batch Firehose sent us
    for record in event['records']:
        
        try:
            # Step 1: Decode the record from base64 to a string
            # Firehose always base64 encodes data before passing to Lambda
            payload = base64.b64decode(record['data']).decode('utf-8')
            
            # Step 2: Parse the JSON string into a Python dictionary
            order = json.loads(payload)
            
            # Step 3: Enrich the record — add extra fields
            order['processed_at'] = datetime.utcnow().isoformat()  # When Lambda processed it
            order['source'] = 'kinesis-streaming'                   # Mark it as streaming data
            order['pipeline_version'] = 'v2'                        # Phase 2 of our portfolio
            
            # Step 4: Convert back to JSON string, then base64 encode it
            # Firehose expects the transformed data back in base64 format
            enriched_payload = json.dumps(order)
            encoded_payload = base64.b64encode(enriched_payload.encode('utf-8')).decode('utf-8')
            
            # Step 5: Append to output with status 'Ok'
            output_records.append({
                'recordId': record['recordId'],  # Must match the original recordId
                'result': 'Ok',
                'data': encoded_payload
            })
        
        except Exception as e:
            # If something goes wrong processing a record, mark it as failed
            # Firehose will send failed records to a separate S3 error prefix
            print(f"Error processing record: {e}")
            output_records.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']  # Send original data back unchanged
            })
    
    print(f"Processed {len(output_records)} records successfully.")
    
    # Return all transformed records back to Firehose
    return {'records': output_records}