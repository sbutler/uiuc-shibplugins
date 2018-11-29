from datetime import datetime, timezone, timedelta

from . import ToolTestCase

class UpdateTestCase(ToolTestCase):
    SETUP_BATCH_WRITES = [
        {'PutRequest': {'Item': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is a test string'},
            'Version': {'N': '1'},
        }}},
        {'PutRequest': {'Item': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'expiredKey'},
            'Expires': {'N': '1'},
            'Value': {'S': 'this is an expired string'},
            'Version': {'N': '1'},
        }}},
    ]
    TEARDOWN_BATCH_WRITES = [
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
        }}},
        {'DeleteRequest': {'Key': {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'expiredKey'},
        }}},
    ]


    def test_updateString(self):
        result = self.tool(
            'updateString',
            'testContext',
            'testKey',
            'this is an updated value'
        )

        self.assertTrue(result['result'])
        self.assertEqual(result['version'], 2)

        result = self.tool(
            'updateString',
            'testContext',
            'doesNotExistKey',
            'this is an updated value'
        )

        self.assertFalse(result['result'])
        self.assertEqual(result['version'], 0)

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is an updated value'},
            'Version': {'N': '2'},
        })

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'doesNotExistKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {})

    def test_updateStringExpired(self):
        result = self.tool(
            'updateString',
            'testContext',
            'expiredKey',
            'this is an updated value'
        )

        self.assertFalse(result['result'])
        self.assertEqual(result['version'], 0)

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'expiredKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'expiredKey'},
            'Expires': {'N': '1'},
            'Value': {'S': 'this is an expired string'},
            'Version': {'N': '1'},
        })

    def test_updateStringWithExpires(self):
        expires = int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp())

        result = self.tool(
            'updateString',
            'testContext',
            'testKey',
            'this is an updated value',
            expiration=expires
        )

        self.assertTrue(result['result'])
        self.assertEqual(result['version'], 2)

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': str(expires)},
            'Value': {'S': 'this is an updated value'},
            'Version': {'N': '2'},
        })

    def test_updateStringWithVersionEq(self):
        result = self.tool(
            'updateString',
            'testContext',
            'testKey',
            'this is an updated value',
            version=1
        )

        self.assertTrue(result['result'])
        self.assertEqual(result['version'], 2)

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is an updated value'},
            'Version': {'N': '2'},
        })

    def test_updateStringWithVersionNe(self):
        result = self.tool(
            'updateString',
            'testContext',
            'testKey',
            'this is an updated value',
            version=9999
        )

        self.assertFalse(result['result'])
        self.assertEqual(result['version'], -1)

        result = self.dyndb_clnt.get_item(
            TableName=self.TOOL_TABLE,
            Key={'Context': {'S': 'testContext'}, 'Key': {'S': 'testKey'}},
            ConsistentRead=True
        )
        self.assertEqual(result.get('Item', {}), {
            'Context': {'S': 'testContext'},
            'Key': {'S': 'testKey'},
            'Expires': {'N': '2147483647'},
            'Value': {'S': 'this is a test string'},
            'Version': {'N': '1'},
        })
