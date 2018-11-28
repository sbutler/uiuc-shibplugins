#!/usr/bin/env python3

import os
import unittest
import warnings

warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

suit = unittest.defaultTestLoader.discover(
    os.path.dirname(os.path.abspath(__file__))
)
runner = unittest.TextTestRunner()
runner.run(suit)
