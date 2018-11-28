from datetime import datetime, timezone, timedelta

from . import ToolTestCase

class ReadTestCase(ToolTestCase):
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

    def test_readString(self):
        result = self.tool(
            'readString',
            'testContext',
            'testKey'
        )

        self.assertTrue(result['result'])
        self.assertEqual(result['value'], 'this is a test string')
        self.assertEqual(result['expiration'], 2147483647)
        self.assertEqual(result['version'], 1)

    def test_readStringExpired(self):
        result = self.tool(
            'readString',
            'testContext',
            'expiredKey'
        )

        self.assertFalse(result['result'])

    def test_readStringMissing(self):
        result = self.tool(
            'readString',
            'testContext',
            'doesNotExistKey'
        )

        self.assertFalse(result['result'])

    def test_readString(self):
        result = self.tool(
            'readString',
            'testContext',
            'testKey'
        )

    def test_readStringVersion(self):
        result = self.tool(
            'readString',
            'testContext',
            'testKey',
            version=1
        )

        self.assertTrue(result['result'])
        self.assertNotIn('value', result)
        self.assertEqual(result['expiration'], 2147483647)
        self.assertEqual(result['version'], 1)

        result = self.tool(
            'readString',
            'testContext',
            'testKey',
            version=2
        )

        self.assertTrue(result['result'])
        self.assertEqual(result['value'], 'this is a test string')
        self.assertEqual(result['expiration'], 2147483647)
        self.assertEqual(result['version'], 1)
