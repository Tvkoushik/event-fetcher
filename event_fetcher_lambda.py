import boto3
import time
import os

def lambda_handler(event, context):
    ec2 = boto3.client('ec2')
    ssm = boto3.client('ssm')

    INSTANCE_ID = os.environ['EC2_INSTANCE_ID']
    BUCKET_NAME = os.environ['BUCKET_NAME']
    BUCKET_PATH = os.environ['BUCKET_PATH']
    MAJOR_EVENTS_S3_BUCKET_NAME = os.environ['MAJOR_EVENTS_S3_BUCKET_NAME']
    MAJOR_EVENTS_CSV_FILE_PATH = os.environ['MAJOR_EVENTS_CSV_FILE_PATH']
    
    ssm_script = [
        f"/usr/bin/python3 /home/ubuntu/event-fetcher/events.py {BUCKET_NAME} {BUCKET_PATH}"
    ]

    # Start the EC2 instance
    ec2.start_instances(InstanceIds=[INSTANCE_ID])
    print(f'Started EC2 instance: {INSTANCE_ID}')

    # Wait for the EC2 instance to enter the running state
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[INSTANCE_ID])
    print(f'EC2 instance is up and running: {INSTANCE_ID}')

    # Add a delay to give the EC2 instance time to finish initializing
    time.sleep(15)  # Adjust this value as needed
    
    # Execute the SSM script
    response = ssm.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName='AWS-RunShellScript',
        Parameters={'commands': ssm_script},
    )

    # Get the command ID
    command_id = response['Command']['CommandId']

    # Wait for the command to complete
    while True:
        response = ssm.list_commands(CommandId=command_id)

        if response['Commands'][0]['Status'] not in ['Pending', 'InProgress']:
            break

        print('SSM command still in progress. Waiting...')
        time.sleep(20)

    print(f'SSM command completed with status: {response["Commands"][0]["Status"]}')

    # Stop the EC2 instance
    ec2.stop_instances(InstanceIds=[INSTANCE_ID])
    print(f'Stopped EC2 instance: {INSTANCE_ID}')