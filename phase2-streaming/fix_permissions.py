import boto3
import json

iam = boto3.client('iam', region_name='us-east-1')

policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": "arn:aws:lambda:us-east-1:755352605024:function:kuda-firehose-transformer"
        }
    ]
}

response = iam.put_role_policy(
    RoleName='FirehoseDeliveryRole',
    PolicyName='InvokeLambdaPolicy',
    PolicyDocument=json.dumps(policy_document)
)

print("InvokeLambdaPolicy attached to FirehoseDeliveryRole successfully!")