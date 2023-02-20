### Q1 - get user by user/tenantid:

```python

import os
from shared.dynamodb.main import get_item, Key


def handler(event, context):
    return get_item(
        Key(key='tenant_id', value='99'),
        Key(key='user_id', value='99'),
        os.getenv('table_name')
    )
```

### Q2 - query the number of tenants active in the current month:

```python
import os
from shared.dynamodb.main import select_items, Key


def handler(event, context):
    return len(select_items(
        Key(key='tenant_id', value='99'),
        os.getenv('table_name')
    ))
```

### Q3 - Update user - increment their activity counter by 1:

```python
import os
from shared.dynamodb.main import arcadia_update_counter_ind, Key


def handler(event, context):
    return arcadia_update_counter_ind(Key('tenant_id', '99'), Key('user_id', '99'), os.getenv('table_name'))
```

### Q4 - Set activity counter of all items to 0 for all records:
```python
# TODO: Fun task for Robert :P to handle the quering part of this problem and than run for loop on the items returned to reset the with the code below
# use dynamo tools.query into the added update item function or write your own it is just a stylistic wrapper, writing your own function might be a better practice imho.
import os
from shared.dynamodb.main import arcadia_counter_ind_reset, Key


def handler(event, context):
    return arcadia_counter_ind_reset(Key('tenant_id', '99'), Key('user_id', '99'), os.getenv('table_name'))

```


### Q5 Clear table - truncate

```python
import os
from shared.dynamodb.main import truncate_table


def handler(event, context):
    return truncate_table(os.getenv('table_name'))
```