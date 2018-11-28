from datetime import datetime, timezone, timedelta

from . import ToolTestCase

class CreateTestCase(ToolTestCase):
    TEARDOWN_BATCH_WRITES = [
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
        }}},
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey2'},
        }}},
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'testContext2'},
            'Key': {'S': 'testKey'},
        }}},
    ]

    def test_createString(self):
        expires = int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp())

        result = self.tool(
            'createString',
            'testContext',
            'testKey',
            'this is a test value',
            expires
        )
        self.assertTrue(result['result'])

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': str(expires)},
            'Value': {'S': 'this is a test value'},
            'Version': {'N': '1'},
        })

    def test_createStringExpired(self):
        expired = int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp())
        expires = int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp())

        self.dyndb_clnt.put_item(
            TableName=self.TOOL_TABLE,
            Item={
                'Context': {'S': 'testContext'},
                'Key': {'S': 'testKey'},
                'Expires': {'N': str(expired)},
                'Value': {'S': 'this is an expired value'},
                'Version': {'N': '1'},
            }
        )

        result = self.tool(
            'createString',
            'testContext',
            'testKey',
            'this is a test value',
            expires
        )
        self.assertTrue(result['result'])

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': str(expires)},
            'Value': {'S': 'this is a test value'},
            'Version': {'N': '1'},
        })

    def test_createStringUnique(self):
        expires = int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp())

        self.dyndb_clnt.put_item(
            TableName=self.TOOL_TABLE,
            Item={
                'Context': {'S': 'testContext'},
                'Key': {'S': 'testKey'},
                'Expires': {'N': str(expires)},
                'Value': {'S': 'this is an existing value'},
                'Version': {'N': '1'},
            }
        )

        result = self.tool(
            'createString',
            'testContext',
            'testKey',
            'this is a test value',
            expires
        )
        self.assertFalse(result['result'])

        result = self.tool(
            'createString',
            'testContext2',
            'testKey',
            '2this is a test value',
            expires
        )
        self.assertTrue(result['result'])

        result = self.tool(
            'createString',
            'testContext',
            'testKey2',
            'this is a test value2',
            expires
        )
        self.assertTrue(result['result'])

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': str(expires)},
            'Value': {'S': 'this is an existing value'},
            'Version': {'N': '1'},
        })

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'testKey2'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey2'},
            'Expires': {'N': str(expires)},
            'Value': {'S': 'this is a test value2'},
            'Version': {'N': '1'},
        })

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext2'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext2'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': str(expires)},
            'Value': {'S': '2this is a test value'},
            'Version': {'N': '1'},
        })