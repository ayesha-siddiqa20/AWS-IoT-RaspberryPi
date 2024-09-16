import json
import boto3
from decimal import Decimal

sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('RaspberryPiData')

# Replace with your SNS topic ARN
SNS_TOPIC_ARN = ''

def lambda_handler(event, context):
    try:
        # Convert Fahrenheit to Celsius
        temperature_celsius = (event['Temperature'] - 32) * 5/9
        
        # Determine the message based on temperature
        if 24 <= temperature_celsius <= 25:
            message = "Is it me or is the AC off?"
        elif temperature_celsius > 25:
            message = "The AC is definitely off."
        else:
            message = None
        
        # Send the message if applicable
        if message:
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message
            )
        
        # Convert float values to Decimal for DynamoDB
        item = {
            'ID': event['ID'],
            'Temperature': Decimal(str(event['Temperature'])),
            'Humidity': Decimal(str(event['Humidity'])),
            'Pressure': Decimal(str(event['Pressure'])),
            'Timestamp': event['Timestamp']
        }
        
        # Insert the data into the DynamoDB table
        table.put_item(Item=item)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data processed successfully')
        }
    except Exception as e:
        print(f"Error processing event: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing data')
        }
