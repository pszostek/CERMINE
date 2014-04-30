#!/usr/bin/env python

""" Mirror PMC ftp in a parallel fashion """

from multiprocessing import Pool
from subprocess import call
import sys

PMC = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/"
NUMBER_OF_THREADS = 47


def mirror(url):
    print(url)
    call("wget --mirror " + PMC + url, shell=True)
		

if __name__ == "__main__":
    file = sys.argv[1]
    file = open(file, "r")
    files = [f.strip() for f in file.readlines()]
    pool = Pool(processes=NUMBER_OF_THREADS)
    urls = ("%02x" % integer for integer in reversed(xrange(11, 256)))
    pool.map(mirror,  urls)
    pool.close()
    pool.join()
