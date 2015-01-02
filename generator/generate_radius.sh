#!/usr/bin/env python

import sys
import random

if (len(sys.argv) != 4):
	print "Usage: $> generate.sh radiusfilename centroidfilename range"
	sys.exit(0)

radiusfile = open(sys.argv[1], 'w')
centroidfile = open(sys.argv[2], 'r')

centroid_count = len(centroidfile.readlines() )

for i in range(0, centroid_count):
	radiusfile.write(str(random.uniform(0, int(sys.argv[3]) ) ) + "\n")
