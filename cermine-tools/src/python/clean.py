#!/usr/bin/env python

from multiprocessing import Pool
from functools import partial
import os, re
import tarfile
import logging
import libxml2

""" Walks the whole nlm FTP mirror and extracts found .tar.gz files in their locations,
    remove files different than .pdf and .xml, renames the directory to get rid of
    non-standard characters. The directory is remove if there is no .xml file"""

NUMBER_OF_PROCESSES = 47

PMC = "ftp.ncbi.nlm.nih.gov"

TARGZ_RE = re.compile(r".*\.tar\.gz")
PDF_RE = re.compile(r".*\.pdf")
NLM_RE = re.compile(r".*\.nxml")

logger = logging.getLogger('tcpserver') 

def get_nlm_id(nlm_path):
    doc = libxml2.parseFile(nlm_path)
    context = doc.xpathNewContext()
    res = context.xpathEval("/article/front/article-meta/article-id[@pub-id-type='pmc']")
    nlm_id = res[0].getContent()
    return nlm_id

def corename(filename):
    return '.'.join(filename.split('.')[:-1])
    
def targz_corename(filename):
    return '.'.join(filename.split('.')[:-2])

def treat_targz(path_prefix, targz_name):
    targz_path = os.path.join(path_prefix, targz_name)
    targz = tarfile.open(targz_path, "r:gz")
    pdf = None
    nlm = None
    for item in targz:
        if PDF_RE.match(item.name):
            pdf = item.name
        if NLM_RE.match(item.name):
            nlm = item.name
    if nlm != None and pdf != None:
        targz.extract(pdf.name, path_prefix)
        targz.extract(nlm.name, path_prefix)
        targz.close()
        os.remove(targz_path)

        dirname = targz_corename(targz_name)
        new_dirname = re.sub(r'[^a-zA-Z0-9_-]', r'_', dirname)

        print("Rename %s/%s" % (path_prefix, dirname) + "to %s/%s" % (path_prefix, new_dirname))
        os.rename("%s/%s" % (path_prefix, dirname), "%s/%s" % (path_prefix, new_dirname))

        nlm_path = os.path.join(path_prefix, new_dirname, nlm)
        pdf_path = os.path.join(path_prefix, new_dirname, pdf)
        
        nlm_id = get_nlm_id(nlm_path)
        os.rename(nlm_path, os.path.join(path_prefix, new_dirname, "%s.nxml" % nlm_id))
        os.rename(pdf_path, os.path.join(path_prefix, new_dirname, "%s.pdf" % nlm_id))
    else:
        targz.close()
        # remove the directory if the is no nlm
        remove_dir = "%s/%s" % (path_prefix, dirname)
        logger.info("Removing directory %s" % remove_dir)
        os.rmdir(remove_dir)


def clean(path_prefix):
	path_prefix = "%s/pub/pmc/%s" % (PMC, path_prefix)
	if(os.path.exists(path_prefix)):
		try:
			os.remove("%s/.listing" % path_prefix)
		except:
			pass
		files = os.listdir(path_prefix)
		targzs = (f for f in files if TARGZ_RE.match(f))
		map(partial(treat_targz, path_prefix), targzs)

if __name__ == "__main__":
    path_prefixes = ["%02x/%02x" % (x, y) for x in xrange(0, 256) for y in xrange(0, 256)]
    pool = Pool(processes=NUMBER_OF_PROCESSES)
    pool.map(clean, path_prefixes)
    pool.close()
    pool.join()
