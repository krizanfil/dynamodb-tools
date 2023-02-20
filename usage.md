## insert_item usage:

```python
import os
import boto3
from dynamodb.insert import insert_item

def handler(event, context):
    dynamo_db = boto3.resource('dynamodb')
    table_name = os.getenv('table_name')
    return [insert_item(item, table_name, dynamo_db) for item in
            [{'tenant_id': f'{x}', 'user_id': f'{x}'} for x, in range(120)]]
```

## get_item usage:

```python
import os
from dynamodb.dt_select import get_item
from dynamodb.common import Key


def handler(event, context):
    primary_key, sort_key = Key('tenant_id', '99'), Key('user_id', '99')
    item = get_item(primary_key, sort_key, os.getenv('table_name'))

    return item
```

## scan_table usage:

```python
import os
from dynamodb.dt_select import scan_table


def handler(event, context):
    items = scan_table(os.getenv('table_name'))

    return items
```

## select_items usage:

```python
import os
from dynamodb.common import Key
from dynamodb.delete import select_items


def handler(event, context):
    return select_items(Key('tenant_id', '99'), os.getenv('table_name'))
```

## truncate_table usage:

```python
import os
from dynamodb.main import truncate_table


def handler(event, context):
    return truncate_table(os.getenv('table_name'))
```
