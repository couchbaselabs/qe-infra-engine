import sys
import os
script_dir = os.path.dirname(os.path.realpath(__file__))
paths = [
    os.path.join(script_dir, "..", "..", "util", "ssh_util"),
]
for path in paths:
    sys.path.append(path)