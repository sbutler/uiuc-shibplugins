from datetime import datetime, timezone, timedelta

from . import ToolTestCase

class DeleteContextTestCase(ToolTestCase):
    SETUP_BATCH_WRITES = [
        {'PutRequest': {'Item': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey1'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is a test string'},
            'Version': {'N': '1'},
        }}},
        {'PutRequest': {'Item': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey2'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is a test string'},
            'Version': {'N': '1'},
        }}},
        {'PutRequest': {'Item': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey3'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is a test string'},
            'Version': {'N': '1'},
        }}},
        {'PutRequest': {'Item': {
            'Context': {'S': 'unchangedContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is a test string'},
            'Version': {'N': '1'},
        }}},
    ]
    TEARDOWN_BATCH_WRITES = [
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey1'},
        }}},
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey2'},
        }}},
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey3'},
        }}},
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'unchangedContext'},
            'Key': {'S': 'testKey'},
        }}},
    ]


    def test_deleteContext(self):
        result = self.tool(
            'deleteContext',
            'testContext'
        )

        self.assertTrue(result['result'])

        for k in ('testKey1', 'testKey2', 'testKey3'):
            result = self.dyndb_clnt.get_item(
                TableName=self.TOOL_TABLE,
                Key={'Context': {'S': 'testContext'}, 'Key': {'S': k}},
                ConsistentRead=True
            )
            self.assertEqual(result.get('Item', {}), {})

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'unchangedContext'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'unchangedContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is a test string'},
            'Version': {'N': '1'},
        })
