from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator
import pathlib
from dotenv import dotenv_values
import json
from tempfile import NamedTemporaryFile

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

def invoke_with_operator(target_name: str):
  EVENT = {
      "Records": [
        {
          "s3": {
            "bucket": {
              "name": config['bucket_name']
            },
            "object": {
              "key": target_name
            }
          }
        }
      ]
    }
  print(AwsLambdaInvokeFunctionOperator(
        task_id="invoke_lambda_function",
        function_name=config['lambda_function_name'],
        payload=EVENT,
    ))
  return EVENT