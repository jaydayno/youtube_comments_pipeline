from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.hooks.S3_hook import S3Hook
from dotenv import dotenv_values
import pathlib
import json
import logging

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

def invoke_with_operator(ti, target_name: str, stage_name: str):
    BUCKET_NAME = config['bucket_name']
    s3 = S3Hook(aws_conn_id='aws_default')

    if s3.check_for_key(key=stage_name, bucket_name=BUCKET_NAME) == False:    
        event_dict = {"Records": [{"s3": {"bucket": {"name": config['bucket_name']}, "object": {"key": target_name}}}]}
        event_str = json.dumps(event_dict)
        success_status_codes = [200, 202, 204]
        hook = LambdaHook(aws_conn_id='aws_default')
        logging.info("Invoking AWS Lambda function: %s with payload: %s", config['lambda_function_name'], event_str)
        response = hook.invoke_lambda(
                    function_name=config['lambda_function_name'],
                    payload=event_str
                )
        logging.info("Lambda response metadata: %r", response.get("ResponseMetadata"))
        if response.get("StatusCode") not in success_status_codes:
            raise ValueError(f'Lambda function got this response: {response.get("StatusCode")} did not execute', json.dumps(response.get("ResponseMetadata")))
        payload_stream = response.get("Payload")
        payload = payload_stream.read().decode()
        if "FunctionError" in response:
            raise ValueError(
                'Lambda function execution resulted in error',
                {"ResponseMetadata": response.get("ResponseMetadata"), "Payload": payload},
            )
        logging.info('Lambda function invocation succeeded: %r', response.get("ResponseMetadata"))
        ti.xcom_push(key='BUCKET_NAME', value=config['bucket_name'])
        return payload
    else:
        logging.info(f"ALREADY UPLOADED to S3 bucket: {BUCKET_NAME} with name {stage_name}")
        return False