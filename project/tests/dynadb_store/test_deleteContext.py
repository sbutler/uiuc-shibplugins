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
