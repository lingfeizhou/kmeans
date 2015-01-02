#!/usr/bin/env python

import sys
import random

if (len(sys.argv) != 5):
	print "Usage: $> generate.sh centroidfilename points dimension range"
	sys.exit(0)

centroidfile = open(sys.argv[1], 'w')

for i in range(0, int(sys.argv[2]) ):
	centroidfile.write(str(i) + " ")
	for j in range(0, int(sys.argv[3])):
		centroidfile.write(str(random.uniform(0 - int(sys.argv[4]), int(sys.argv[4]) ) ) + " ")
	centroidfile.write("\n")
