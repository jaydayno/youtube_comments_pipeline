from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
import pathlib
from dotenv import dotenv_values
import json
import logging

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

def invoke_with_operator(ti, target_name: str):
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
        raise ValueError('Lambda function did not execute', json.dumps(response.get("ResponseMetadata")))
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