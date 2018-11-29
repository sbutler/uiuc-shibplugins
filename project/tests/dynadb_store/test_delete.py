from datetime import datetime, timezone, timedelta

from . import ToolTestCase

class DeleteTestCase(ToolTestCase):
    SETUP_BATCH_WRITES = [
        {'PutRequest': {'Item': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is a test string'},
            'Version': {'N': '1'},
        }}},
    ]
    TEARDOWN_BATCH_WRITES = [
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
        }}},
    ]

    def test_deleteString(self):
        result = self.tool(
            'deleteString',
            'testContext',
            'testKey'
        )

        self.assertTrue(result['result'])
        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {})
