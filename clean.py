#!/usr/bin/python3

import os
for parent, dirnames, filenames in os.walk('.'):
    for fn in filenames:
        if fn.lower().endswith('.db'):
            os.remove(os.path.join(parent, fn))
        if fn.lower().endswith('.log'):
            os.remove(os.path.join(parent, fn))
        if fn.lower().endswith('.db-journal'):
            os.remove(os.path.join(parent, fn))