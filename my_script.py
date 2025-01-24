import os
import sys
import boto3
import botocore
import json
from datetime import datetime

import botocore.exceptions

# boto3 clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
dynamodb_client = boto3.client('dynamodb')

def handler(event, context):
    # Obtain the message from the SQS queue
    try:

        print(f"Event: {event}")
        body = json.loads(event['Records'][0]["body"])

        if not "Records" in body:            
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

        raise e

    except json.JSONDecodeError as e:
        print(f"[ERROR] There was an error decoding the message, check syntax of file: {str(e)}")

        raise e
    
    except TypeError as e:
        print(f"[ERROR] There was an error while gathering the messages: {str(e)}")

        raise e

    except Exception as e:
        print(f"[ERROR] There was an unexpected error while gathering the messages: {str(e)}")

        raise e
    
    event_type = body["Records"][0]["eventName"]
    print(f"Event type: {event_type}")

    bucket_name = body["Records"][0]["s3"]["bucket"]["name"]
    object_key = body["Records"][0]["s3"]["object"]["key"]
    s3_path = f"s3://{bucket_name}/{object_key}"
    print(f"S3 path built: {s3_path}")

    if object_key.endswith("/"):
        print(f"[INFO] The object key is a folder, skipping the processing")

        message = {
            'status': 'success',
            'message': 'The object key is a folder, skipping the processing'
        }

        return {
            'statusCode': 201,
            'body': json.dumps(message)
        }

    # Download the file from S3
    try:
        print(f"Downloading file from S3: {s3_path}")

        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read() # NOTE: Bytes (I think)

        print(f"File downloaded from S3: {s3_path}")

    except botocore.exceptions.ClientError as e:
        print(f"[ERROR] There was an error while downloading the file from S3: {str(e)} -- Probably the file doesn't exist")

        raise e

    except Exception as e:
        print(f"[ERROR] There was an unexpected error while downloading the file from S3: {str(e)}")

        raise e
    
    # Process the file (file_content)
    try:
        extracted_data = {
            "fecha_denuncia": "07/09/2023",
            "hora_denuncia": "09:32",
            "organismo": "Stadt Dachau, Kommunale Verkehrsüberwachung",
            "matricula": "MRL1146",
            "expediente": "103951099604",
            "articulo_infraccion": "25.00.00X",
            "hecho_denunciado": "Verkehrsordnungswidrigkeit",
            "lugar_infraccion": "Dachau",
            "sancion": 23.5,
            "importe_reducido": 0,
            "puntos_carnet": 0,
            "retirada_de_carnet": False,
            "denunciado_nombre": "Othman Ktiri Cars Deutschland GmbH",
            "denunciado_nif": None,
            "gravedad": "Leve",
            "ide_o_rec": "Identificacion",
            "plazo_dias": 14,
            "plazo_son_dias_habiles": False,
            "plazo_desde_recepcion": True,
            "pais_origen": "Alemania",
            "is_peaje": False
        }

        print(f"File processed successfully")

    except Exception as e:
        print(f"[ERROR] There was an unexpected error while extracting the data: {str(e)}")

        raise e

    # Generate unique id and timestamp
    item_id = object_key  # Path to the file in S3 without the bucket name
    timestamp = datetime.utcnow().isoformat()  # Current timestamp in ISO 8601 format

    # Build the DynamoDB item
    item = {
        "id": {"S": item_id},  # Hash key
        "timestamp": {"S": timestamp}  # Range key
    }

    # Cast data types to DynamoDB format
    for key, value in extracted_data.items():
        if isinstance(value, str):
            print(f"String value: {value}")
            item[key] = {"S": str(value)}  # String
        elif isinstance(value, bool):
            print(f"Boolean value: {value}")
            item[key] = {"BOOL": value}  # Boolean
        elif isinstance(value, (int, float)):
            print(f"Number value: {value}")
            item[key] = {"N": str(value)}  # Number
        elif value is None:
            print(f"Null value: {value}")
            item[key] = {"NULL": True}  # Null value

    # Send the extracted data to control_queue SQS
    try:
        print(f"Sending extracted data to the control queue")
        control_queue_url = os.getenv('SQS_CONTROL_QUEUE_URL')

        response = sqs_client.send_message(
            QueueUrl=control_queue_url,
            MessageBody=json.dumps(extracted_data)
        )

        print(f"Extracted data sent to the control queue")

    except botocore.exceptions.ClientError as e:
        print(f"[ERROR] There was an error while sending the extracted data to the control queue: {str(e)}")

        raise e

    except Exception as e:
        print(f"[ERROR] There was an unexpected error while sending the extracted data to the control queue: {str(e)}")

        raise e
    
    # Upsert extracted data to the DynamoDB table
    try:
        print(f"Upserting extracted data to the DynamoDB table")
        dynamodb_table_name = os.getenv('DYNAMODB_TABLE_NAME')

        # Perform the upsert operation
        response = dynamodb_client.put_item(
            TableName=dynamodb_table_name,
            Item=item
        )

        print(f"Extracted data upserted to the DynamoDB table")

    except botocore.exceptions.ClientError as e:
        print(f"[ERROR] There was an error while upserting the extracted data to the DynamoDB table: {str(e)}")

        raise e

    except Exception as e:
        print(f"[ERROR] There was an unexpected error while upserting the extracted data to the DynamoDB table: {str(e)}")

        raise e

    message = {
            'status': 'success',
            'message': f"File {object_key} processed successfully"
        }

    return {
        'statusCode': 200,
        'body': json.dumps(message)
    }
