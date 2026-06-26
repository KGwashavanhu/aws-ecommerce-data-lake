import boto3
import json
import time

# boto3 clients for Step Functions and IAM
sfn = boto3.client('stepfunctions', region_name='us-east-1')
iam = boto3.client('iam', region_name='us-east-1')

ROLE_NAME = 'StepFunctionsRole'
STATE_MACHINE_NAME = 'kuda-ecommerce-pipeline'

def create_step_functions_role():
    """
    Creates an IAM role that Step Functions will assume when executing.
    Step Functions needs permission to invoke Lambda and start Glue crawlers.
    """
    
    # Trust policy — allows Step Functions service to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        role = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Allows Step Functions to orchestrate the ecommerce pipeline'
        )
        print(f"IAM role '{ROLE_NAME}' created.")
        
        # Permission policy — Step Functions needs to invoke Lambda and run Glue crawlers
        permission_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "lambda:InvokeFunction"
                    ],
                    "Resource": "arn:aws:lambda:us-east-1:755352605024:function:kuda-firehose-transformer"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:StartCrawler",
                        "glue:GetCrawler",
                        "glue:GetCrawlerMetrics"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogDelivery",
                        "logs:PutLogEvents"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
        iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName='StepFunctionsExecutionPolicy',
            PolicyDocument=json.dumps(permission_policy)
        )
        print("Permissions attached to role.")
        
        print("Waiting for role to propagate...")
        time.sleep(10)
        
        return role['Role']['Arn']
    
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"Role '{ROLE_NAME}' already exists, fetching ARN...")
        role = iam.get_role(RoleName=ROLE_NAME)
        return role['Role']['Arn']

def create_state_machine(role_arn):
    """
    Creates the Step Functions state machine.
    
    A state machine is a workflow made up of states — each state does something
    (invoke Lambda, wait, check a condition) and then transitions to the next state.
    
    Our pipeline has these steps:
    1. WaitForFirehose — waits 90 seconds for Firehose to flush records to S3
    2. StartGlueCrawler — triggers the Glue crawler to catalog new streaming data
    3. WaitForCrawler — waits 60 seconds for the crawler to finish
    4. PipelineComplete — marks the execution as successful
    """
    
    # State machine definition in Amazon States Language (ASL)
    # This is a JSON format that describes the workflow
    definition = {
        "Comment": "Kuda ecommerce streaming pipeline orchestrator",
        "StartAt": "WaitForFirehose",
        "States": {
            
            # State 1: Wait for Firehose to deliver records to S3
            # Firehose buffers for 60 seconds so we wait 90 to be safe
            "WaitForFirehose": {
                "Type": "Wait",
                "Seconds": 90,
                "Next": "StartGlueCrawler"
            },
            
            # State 2: Start the Glue crawler to catalog the new S3 data
            "StartGlueCrawler": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
                "Parameters": {
                    "Name": "kuda-streaming-crawler"  # We'll create this crawler next
                },
                "Next": "WaitForCrawler",
                "Catch": [
                    {
                        # If crawler is already running, just move on
                        "ErrorEquals": ["Glue.CrawlerRunningException"],
                        "Next": "WaitForCrawler"
                    }
                ]
            },
            
            # State 3: Wait for the crawler to finish cataloging
            "WaitForCrawler": {
                "Type": "Wait",
                "Seconds": 60,
                "Next": "PipelineComplete"
            },
            
            # State 4: Final state — marks the execution as successful
            "PipelineComplete": {
                "Type": "Succeed"
            }
        }
    }
    
    try:
        response = sfn.create_state_machine(
            name=STATE_MACHINE_NAME,
            definition=json.dumps(definition),
            roleArn=role_arn,
            type='STANDARD'  # STANDARD supports longer running workflows (up to 1 year)
        )
        print(f"State machine '{STATE_MACHINE_NAME}' created successfully.")
        print(f"ARN: {response['stateMachineArn']}")
        return response
    
    except sfn.exceptions.StateMachineAlreadyExists:
        print(f"State machine '{STATE_MACHINE_NAME}' already exists.")

if __name__ == "__main__":
    print("Setting up Step Functions state machine...\n")
    role_arn = create_step_functions_role()
    create_state_machine(role_arn)
    print("\nStep Functions setup complete!")