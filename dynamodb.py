import boto3
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Union, Literal
from boto3.dynamodb.types import TypeDeserializer


@dataclass
class Key:
    key: str
    value: any


def get_inserter(key: str, value: any):
    if isinstance(value, (int, float)):
        return f"'{key}' : {value},"
    return f"'{key}' : '{value}',"


def generate_partiql_insert_statement(item: Dict[str, any], table_name: str) -> str:
    """
    Function that for given item formulates PartiQl statement and returns it
    in a form of string that can be later executed/batch_executed via the
    execute_partiql_statement/batch_execute_partiql_statement functions.
    Upon execution inserts given item into dynamodb table with the specified table_name.
    """
    statement = f'INSERT INTO "{table_name}" value {{'

    for key, value in item.items():
        statement += get_inserter(key, value)

    return statement.rstrip(",") + "}"


def insert_item(
    item: Dict[str, any],
    table_name: str,
    dynamodb: Optional['boto3.resource'] = boto3.resource('dynamodb')
):
    """
    Inserts item into DynamoDb table
    """
    table = dynamodb.Table(table_name)
    return table.put_item(Item=item)


def query_table(
    table_name: str,
    key_condition_expression: str,
    expression_attribute_values: Dict[str, Dict[str, str]],
    dynamo_db_client: boto3.client = boto3.client('dynamodb'),
    secondary_index_name: Optional[str] = None,
    next_token: Optional[dict] = None,
    limit: int = 100,
) -> List[Dict]:
    """
    Query DynamoDb table using boto3's parameters.
    Returns items in a nice deserialized way.
    """
    try:
        deserializer = TypeDeserializer()
        query_params = {
            "TableName": table_name,
            "KeyConditionExpression": key_condition_expression,
            "ExpressionAttributeValues": expression_attribute_values,
            "Limit": limit,
        }

        if secondary_index_name:
            query_params["IndexName"] = secondary_index_name
        if next_token:
            query_params["ExclusiveStartKey"] = next_token

        response = dynamo_db_client.query(**query_params)
        return [
            {k: deserializer.deserialize(v) for k, v in item.items()}
            for item in response.get('Items', [])
        ]

    except Exception as e:
        logging.error(f"Querying table: {table_name}, failed: {str(e)}")
        raise e


def scan_table(
    table_name: str,
    dynamodb_client: Optional[any] = boto3.client('dynamodb')
) -> List[dict]:
    """
    Returns scan of shared memory dynamoDB table.
    Returns items in a nice deserialized way.
    """
    items = []
    paginator = dynamodb_client.get_paginator("scan")
    deserializer = TypeDeserializer()

    for page in paginator.paginate(TableName=table_name):
        page_items = page.get("Items", [])
        items.extend(
            [
                {k: deserializer.deserialize(v) for k, v in item.items()}
                for item in page_items
            ]
        )

    return items


def select_items(
        primary_key: Key,
        table_name: str,  # Should work with indexes if you look into it
        column_names: Optional[Union[List[str], Literal['*']]] = '*',
        dynamo_db_client: Optional['boto3.client'] = boto3.client('dynamodb'),
) -> List[dict]:
    """
    For specific primary_key value get all items that are stored in the given dynamo table. (Using PartiQl)
    Returns items in a nice deserialized way.
    """
    deserializer = TypeDeserializer()

    if column_names != '*':
        holder = column_names
        column_names = ''
        for name in holder:
            column_names += f'{name}, '
        column_names.rstrip(',').rstrip()

    if not isinstance(primary_key.value, (int, float)):
        primary_key.value = f"'{primary_key.value}'"

    column_names += ' '
    response = dynamo_db_client.execute_statement(
        Statement=f"""SELECT {column_names}FROM "{table_name}" WHERE {primary_key.key} = {primary_key.value}""",
    )

    return [
        {k: deserializer.deserialize(v) for k, v in item.items()}
        for item in response.get('Items', [])
    ]


def get_item(
    primary_key: Key,
    table_name: str,
    sort_key: Optional[Key] = None,
    dynamodb_resource: Optional['boto3.resource("dynamodb").Table'] = boto3.resource("dynamodb")
):
    """
    Gets item with given primary/sort key combination from the table.
    """
    table = dynamodb_resource.Table(table_name)
    key = {
        primary_key.key: primary_key.value,
    }

    if sort_key:
        key[sort_key.key] = sort_key.value

    return table.get_item(Key=key).get('Item')


def truncate_table(table_name: str, dynamo: boto3.resource = boto3.resource('dynamodb')):
    """
    Truncates table by scanning it (returns 1MB chunks) than deleting the chunks as batch operation.
    """
    table = dynamo.Table(table_name)

    # get the table keys
    table_key_names = [key.get("AttributeName") for key in table.key_schema]

    # Only retrieve the keys for each item in the table (minimize data transfer)
    projection_expression = ", ".join("#" + key for key in table_key_names)
    expression_attr_names = {"#" + key: key for key in table_key_names}

    page = table.scan(
        ProjectionExpression=projection_expression,
        ExpressionAttributeNames=expression_attr_names,
    )

    with table.batch_writer() as batch:
        while page["Count"] > 0:
            # Delete items in batches
            for itemKeys in page["Items"]:
                batch.delete_item(Key=itemKeys)
            # Fetch the next page
            if "LastEvaluatedKey" in page:
                page = table.scan(
                    ProjectionExpression=projection_expression,
                    ExpressionAttributeNames=expression_attr_names,
                    ExclusiveStartKey=page["LastEvaluatedKey"],
                )
            else:
                return 'Table successfully truncated'


def delete_item(table_name: str, primary_key: Key, sort_key: Optional[Key] = None):
    """
    Deletes item from DynamoDb.
    """
    key = {
        primary_key.key: primary_key.value,
    }

    if sort_key:
        key[sort_key.key] = sort_key.value

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    return table.delete_item(Key=key)


def increment_counter(
        primary_key: Key,
        sort_key: Key,
        counter_name: str,
        table_name: str,
        dynamodb: Optional['boto3.resource()'] = boto3.resource('dynamodb')
):
    """
    Function with niche usability that increments a counter column inside the DynamoDb table.
    """
    table = dynamodb.Table(table_name)
    return table.update_item(
        Key={
            primary_key.key: primary_key.value,
            sort_key.key: sort_key.value
        },
        UpdateExpression=f'SET {counter_name} = {counter_name} + :val',
        ExpressionAttributeValues={
            ':val': 1
        }
    )


def get_setter(key: str, value) -> str:
    if isinstance(value, (int, float)):
        return f" SET {key} = {value}"
    return f" SET {key} = '{value}'"


def generate_partiql_update_statement(
    primary_key: Key,
    non_key_values: Dict[str, any],
    table_name,
    sort_key: Optional[Key] = None,
) -> str:
    """
        Function that for given item formulates PartiQl statement and returns it
        in a form of string that can be later executed/batch_executed via the
        execute_partiql_statement/batch_execute_partiql_statement functions.
        Upon execution updates given item that already must be inside the dynamodb
        table with the specified table_name.
        """
    statement = f'UPDATE "{table_name}"'

    for key, value in non_key_values.items():
        statement += get_setter(key, value)

    statement += f" WHERE {primary_key.key} = '{primary_key.value}' AND {sort_key.key} = '{sort_key.value}'" if \
        sort_key else f" WHERE {primary_key.key} = '{primary_key.value}''"

    return statement


def execute_partiql_statement(
    statement: str,
    dynamodb_client: 'boto3.client' = boto3.client('dynamodb')
):
    """
    Executes a PartiQL statement.
    """
    return dynamodb_client.execute_statement(Statement=statement)


def batch_execute_partiql_statement(
    statements: List[Dict[str, str]],
    dynamodb_client: 'boto3.client' = boto3.client('dynamodb')
):
    """
    Executes a list of PartiQL statements in a batch.
    """
    return dynamodb_client.batch_execute_statement(Statements=statements)


class DynamoDB:
    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self.client = boto3.client('dynamodb')
        self.resource = boto3.resource('dynamodb')
        self.table = self.resource.Table(table_name)

    def insert_item(self, item: Dict[str, any]):
        return insert_item(item, self.table_name, self.resource)

    def get_item(self, primary_key: Key, sort_key: Key):
        return get_item(primary_key, self.table_name, sort_key, self.resource)

    def scan(self):
        return scan_table(self.table_name, self.client)

    def delete_item(self, primary_key: Key, sort_key: Key):
        return delete_item(self.table_name, primary_key, sort_key)

    def truncate(self):
        return truncate_table(self.table_name, self.resource)
