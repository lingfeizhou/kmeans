#!/usr/bin/env python

import sys
import random

if (len(sys.argv) != 5):
	print "Usage: $> generate.sh outputfilename centroidfilename radiusfilename  points"
	sys.exit(0)

datafile = open(sys.argv[1], 'w')
centroidfile = open(sys.argv[2], 'r')
radiusfile = open(sys.argv[3], 'r')

total_points = int(sys.argv[4])
overall_count = 0

centroids = []

radius = []

centroidlines = centroidfile.readlines();
radiuslines = radiusfile.readlines();

for line in centroidlines:
	centroids.append([])
	numbers = line.split()
	for i in range(1, len(numbers) ):
		if (i != 0):
			centroids[-1].append(float(numbers[i]) )

for line in radiuslines:
	radius.append(float(line) )

num_clusters = len(centroids)
num_average_points = total_points / num_clusters
num_coords = len(centroids[0]);

for i in range(0, num_clusters - 1):
	cluster_points = int(random.uniform(num_average_points * 0.8, num_average_points * 1.2) )
	print "cluster_points: " + str(cluster_points)
	total_points = total_points - cluster_points
	for j in range(0, cluster_points):
		datafile.write(str(overall_count) + " ")
		for k in range(0, num_coords):
			datafile.write(str(random.gauss(centroids[i][k], radius[i]) ) + " ")
		datafile.write("\n")
		overall_count = overall_count + 1;

print "cluster_points: " + str(total_points)
for j in range(0, total_points):
	datafile.write(str(overall_count) + " ")
	for k in range(0, num_coords):
		datafile.write(str(random.gauss(centroids[-1][k], radius[-1]) ) + " ")
	datafile.write("\n")
	overall_count = overall_count + 1;

		
	
