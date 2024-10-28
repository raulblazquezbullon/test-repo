import numpy as np
import boto3
import json

def handler(event, context):
    
    rand = np.random.random()
    print(f"Random number: {rand}")

    # Obtain the message from the SQS queue
    try:

        print(f"Event: {event}")

        body = json.loads(event['Records'][0]["body"])

        if "Records" in body:
            print(f"Received message: {body}")

            message = {
                'status': 'success',
                'message': f"Received message: {body}"
            }

        else:
            print("No messages in the queue")

            message = {
                'status': 'success',
                'message': 'No messages in the queue'
            }

            return {
                'statusCode': 201,
                'body': json.dumps(message)
            }

    except KeyError as e:
        print(f"[ERROR] There wasn't Records or body key inside the SQS message: {str(e)}")

        message = {
            'status': 'error',
            'message': f"There wasn't Records or body key inside the SQS message: {str(e)}"
        }

        return {
            'statusCode': 400,
            'body': json.dumps(message)
        }

    except json.JSONDecodeError as e:
        print(f"[ERROR] There was an error decoding the message, check syntax of file: {str(e)}")

        message = {
            'status': 'error',
            'message': f"There was an error while decoding the message, check syntax of file: {str(e)}"
        }

        return {
            'statusCode': 400,
            'body': json.dumps(message)
        }
    
    except TypeError as e:
        print(f"[ERROR] There was an error while gathering the messages: {str(e)}")

        message = {
            'status': 'error',
            'message': f"There was an error while gathering the messages: {str(e)}"
        }

        return {
            'statusCode': 400,
            'body': json.dumps(message)
        }

    except Exception as e:
        print(f"[ERROR] There was an unexpected error while gathering the messages: {str(e)}")

        message = {
            'status': 'error',
            'message': f"There was an unexpected error while gathering the messages: {str(e)}"
        }

        return {
            'statusCode': 500,
            'body': json.dumps(message)
        }
    
    event_type = body["Records"][0]["eventName"]
    print(f"Event type: {event_type}")

    bucket_name = body["Records"][0]["s3"]["bucket"]["name"]
    object_key = body["Records"][0]["s3"]["object"]["key"]

    s3_path = f"s3://{bucket_name}/{object_key}"

    print(f"S3 path built: {s3_path}")

    message = {
        'status': 'success',
        'message': f"Event type: {event_type}, S3 path: {s3_path}"
    }

    return {
        'statusCode': 200,
        'body': json.dumps(message)
    }