from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.hooks.S3_hook import S3Hook
from dotenv import dotenv_values
import pathlib
import json
import logging

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

def invoke_with_operator(ti, target_name: str, stage_name: str):
    """
    Called in DAG.
    Provides lambda function with "event" (json that holds S3 bucket location and object location).
    Triggers the lambda operator (transforms raw json data into stage csv data and save back into S3).

    Args:
        ti: TaskInstance type from Airflow, needed for pushing xcom to communicate between tasks.
        target_name: Raw data location in S3 (extracts from this location).
        stage_name: Stage data location in S3 (dumps into this location).

    Returns:
        Returns True when invoke was successful.
        Returns False when the stage_name already exists (i.e. already uploaded in S3).

    Raises:
        ValueError: Response was an Error Code from lambda function or received a FunctionError. 
    """
    BUCKET_NAME = config['bucket_name']
    s3 = S3Hook(aws_conn_id='aws_default')

    if s3.check_for_key(key=stage_name, bucket_name=BUCKET_NAME) == False:
        success_status_codes = [200, 202, 204]    
        # Creating Event named as "event_str" to pass onto Lambda Function
        event_dict = {"Records": [{"s3": {"bucket": {"name": config['bucket_name']}, "object": {"key": target_name}}}]}
        event_str = json.dumps(event_dict)

        # Invoke Lambda Function
        hook = LambdaHook(aws_conn_id='aws_default')
        logging.info("Invoking AWS Lambda function: %s with payload: %s", config['lambda_function_name'], event_str)
        response = hook.invoke_lambda(
                    function_name=config['lambda_function_name'],
                    payload=event_str
                )
        logging.info("Lambda response metadata: %r", response.get("ResponseMetadata"))

        # Checking response if it was successful
        if response.get("StatusCode") not in success_status_codes:
            raise ValueError(f'Lambda function got this response: {response.get("StatusCode")} did not execute', json.dumps(response.get("ResponseMetadata")))
        payload_stream = response.get("Payload")
        payload = payload_stream.read().decode()
        if "FunctionError" in response:
            raise ValueError(
                'Lambda function execution resulted in error',
                {"ResponseMetadata": response.get("ResponseMetadata"), "Payload": payload},
                event_dict
            )
        logging.info('Lambda function invocation succeeded: %r', response.get("ResponseMetadata"))

        # Airflow push BUCKET_NAME into xcoms
        ti.xcom_push(key='BUCKET_NAME', value=config['bucket_name'])
        logging.info(payload)
        return True
    else:
        ti.xcom_push(key='BUCKET_NAME', value=config['bucket_name'])
        logging.info(f"ALREADY UPLOADED STAGE DATA to S3 bucket: {BUCKET_NAME} with name {stage_name}")
        return False