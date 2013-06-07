#! /usr/bin/env python
import sys
import base64

print "\n\n"
s = sys.argv[1]
d=base64.decodestring(s + '='*(-len(s) % 4))
print d
