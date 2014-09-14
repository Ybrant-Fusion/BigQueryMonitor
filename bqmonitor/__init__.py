#!/usr/bin/env python

import logging
import sys

# Define the logger
logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger('bqmonitor')
