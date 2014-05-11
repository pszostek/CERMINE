#!/usr/bin/env python

from multiprocessing import Pool
from subprocess import call
from functools import partial
import ftplib as ftp
import os

PMC = "ftp.ncbi.nlm.nih.gov"
def get_local_files(prefix):
	paths = [] 
	try:
		files = os.listdir("%s/pub/pmc/%s" % (PMC, prefix))
		for file in files:
			paths.append("%s/pub/pmc/%s/%s" % (PMC, prefix, file))
		return paths
	except OSError:
		return []
			
if __name__ == "__main__":

	prefixes = ["%02x/%02x" % (x,y) for x in xrange(0,256) for y in xrange(0,256)]

	pool = Pool(processes=48)
	locals = pool.map(get_local_files, prefixes) 
	locals = [item for sublist in locals for item in sublist]
	locals = set(locals)

	pool.close()
	pool.join()

	output = open("harvest2.output", "w")
	for local_file in locals:
		output.write("%s\n" % local_file)
	output.close()
