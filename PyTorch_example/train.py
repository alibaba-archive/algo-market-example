#-*- coding: UTF-8 -*-

import torch
import sys

print(torch.__version__)

for arg in sys.argv:
    print(arg)