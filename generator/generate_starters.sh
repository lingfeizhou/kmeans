#!/usr/bin/env python

import sys
import random
import os

if (len(sys.argv) != 4):
	print "Usage: $> generate.sh starterfilename datafilename centroidfilename"
	sys.exit(0)

starterfile = open(sys.argv[1], 'w')
datafile = open(sys.argv[2], 'r')
centroidfile = open(sys.argv[3], 'r')

num_centroid = len(centroidfile.readlines())

total_bytes = int(os.stat(sys.argv[2]).st_size)

print "total_bytes: " + str(total_bytes)

seek_pos = []

for i in range(0, num_centroid):
	while True:
		random_pos = random.randint(0, total_bytes)
		datafile.seek(random_pos)
		datafile.readline()
		random_pos = datafile.tell()
		if (random_pos not in seek_pos):
			seek_pos.append(random_pos)
			break
	print "current seek_pos: " + str(seek_pos[-1])

for i in range(0, num_centroid):
	datafile.seek(seek_pos[i])
	datafile.readline()
	line = datafile.readline() 
	numbers = line.split()
	starterfile.write(str(i) + " ")
	for j in range(1, len(numbers)):
		starterfile.write(numbers[j] + " ")
	starterfile.write("\n")
	
