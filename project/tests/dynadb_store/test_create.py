# Copyright (c) 2018 University of Illinois Board of Trustees
# All rights reserved.
#
# Developed by:       Technology Services
#                     University of Illinois at Urbana-Champaign
#                     https://techservices.illinois.edu/
#
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal with the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# - Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimers.
# - Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimers in the
#   documentation and/or other materials provided with the distribution.
# - Neither the names of Technology Services, University of Illinois at
#   Urbana-Champaign, nor the names of its contributors may be used to
#   endorse or promote products derived from this Software without
#   specific prior written permission.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR
# ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH THE SOFTWARE.
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
