# AWS Data Pipeline

This repository contains the Python script and the AWS resources configuration for a data pipeline that fetches event data every day at midnight.

## Overview

This data pipeline leverages several AWS services:

1. **Amazon EventBridge**: Triggers the pipeline every day at midnight.
2. **AWS Step Functions**: Coordinates the steps of the pipeline.
3. **AWS Lambda**: Handles the state transitions of the EC2 instance, the execution of the Python script, and fetching major events data.
4. **Amazon EC2**: Runs the Python script.
5. **AWS Systems Manager**: Executes the Python script on the EC2 instance.
6. **Amazon S3**: Stores the resulting CSV files.

The Python script scrapes web data, processes it, and then stores the result in a CSV file in an S3 bucket. The Lambda function fetches major events data directly from another S3 bucket.

## Repository Structure

- `events.py`: This is the Python script that is run on the EC2 instance. It scrapes web data, processes it, and stores the result in a CSV file.
- `lambda_function.py`: This is the code for the Lambda function that handles the state transitions of the EC2 instance, the execution of the Python script, and fetching major events data.
- `state_machine_definition.json`: This is the definition for the Step Functions state machine in Amazon States Language.
- `eventbridge_rule.json`: This is the configuration for the EventBridge rule that triggers the pipeline every day at midnight.

## Setup

To set up the pipeline, follow the detailed instructions in the [documentation](./Documentation.docx).

## Contributions

Contributions are welcome! Please make sure to read the [Contributing Guide](./CONTRIBUTING.md) before making a pull request.

## License

[MIT](./LICENSE)
