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
import boto3
import json
import os
import subprocess
from tempfile import NamedTemporaryFile
import time
from unittest import TestCase
import warnings


class ToolTestCase(TestCase):
    TOOL_BIN = os.environ.get('UIUC_SHIBPLUGINS_STORE', None)
    TOOL_TABLE = os.environ.get('UIUC_SHIBPLUGINS_STORE_TABLE', None)
    TOOL_REGION = os.environ.get('UIUC_SHIBPLUGINS_STORE_REGION', os.environ.get('AWS_DEFAULT_REGION', None))
    TOOL_LIB = os.environ.get('UIUC_SHIBPLUGINS_STORE_LIB', None)

    def setUp(self):
        self.dyndb_clnt = boto3.client('dynamodb')

        if not self.TOOL_BIN:
            raise ValueError('No TOOL_BIN')
        if not self.TOOL_LIB:
            raise ValueError('No TOOL_LIB')
        if not self.TOOL_TABLE:
            raise ValueError('No TOOL_TABLE')
        if not self.TOOL_REGION:
            raise ValueError('No TOOL_REGION')

        self.tool_cfg = NamedTemporaryFile(
            mode='w+',
            encoding='utf-8',
            prefix='uiuc-shibplugins-tool.',
            suffix='.xml',
            delete=False
        )
        self.tool_cfg.write(f"<Storage tableName='{self.TOOL_TABLE}' region='{self.TOOL_REGION}'/>\n")
        self.tool_cfg.flush()

        b_items = list(getattr(self, 'SETUP_BATCH_WRITES', []))
        r_items = []
        while b_items or r_items:
            while b_items and len(r_items) < 25:
                r_items.append(b_items.pop())

            result = self.dyndb_clnt.batch_write_item(RequestItems={self.TOOL_TABLE: r_items})

            r_items = result.get('UnprocessedItems', {}).get(self.TOOL_TABLE, [])
            if r_items:
                time.sleep(1)


    def tearDown(self):
        b_items = list(getattr(self, 'TEARDOWN_BATCH_WRITES', []))
        r_items = []
        while b_items or r_items:
            while b_items and len(r_items) < 25:
                r_items.append(b_items.pop())

            result = self.dyndb_clnt.batch_write_item(RequestItems={self.TOOL_TABLE: r_items})

            r_items = result.get('UnprocessedItems', {}).get(self.TOOL_TABLE, [])
            if r_items:
                time.sleep(1)

        self.tool_cfg = None
        self.dyndb_clnt = None


    def tool(self, command, *args, **kwargs):
        tool_cmd = [
            self.TOOL_BIN,
            '-l', self.TOOL_LIB,
            '-c', self.tool_cfg.name,
            command,
            *[str(i) for i in args],
        ]
        for k, v in kwargs.items():
            tool_cmd.extend([f'--{k}', str(v)])

        result = subprocess.run(
            tool_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding='utf-8',
            timeout=10,
        )

        if result.returncode == 0:
            return json.loads(result.stdout)

        self.fail(f'Tool failed (cmd={result.args!r}; exitcode={result.returncode}): {result.stderr}')
