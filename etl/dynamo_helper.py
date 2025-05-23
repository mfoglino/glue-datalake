import boto3

dynamodb = boto3.resource("dynamodb")


def read_parameter_from_dynamodb(table_name, primary_key):
    # Select your DynamoDB table
    table = dynamodb.Table(table_name)

    # Read the item from the table
    response = table.get_item(Key=primary_key)

    # Check if the item exists in the response
    if "Item" in response:
        return response["Item"]
    else:
        return None


def write_parameter_to_dynamodb(table_name, item):
    # Initialize a session using Amazon DynamoDB
    # Select your DynamoDB table
    table = dynamodb.Table(table_name)

    # Write the item to the table
    table.put_item(Item=item)
