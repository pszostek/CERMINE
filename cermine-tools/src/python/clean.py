#!/usr/bin/env python

from multiprocessing import Pool
from functools import partial
import os
import re
import tarfile
import logging
import libxml2
import shutil
import Queue


""" Walks the whole nlm FTP mirror and extracts found .tar.gz files in their locations,
    remove files different than .pdf and .xml, renames the directory to get rid of
    non-standard characters. The directory is remove if there is no .xml file"""

NUMBER_OF_PROCESSES = 47

PMC = "ftp.ncbi.nlm.nih.gov"

TARGZ_RE = re.compile(r".*\.tar\.gz")
PDF_RE = re.compile(r".*\.pdf")
NLM_RE = re.compile(r".*\.nxml")


def get_nlm_id(nlm_path):
    print("Parsing %s" % nlm_path)
    doc = libxml2.parseFile(nlm_path)
    context = doc.xpathNewContext()
    res = context.xpathEval(
        "/article/front/article-meta/article-id[@pub-id-type='pmc']")
    nlm_id = res[0].getContent()
    print("NLM_ID" + nlm_id)
    return nlm_id


def corename(filename):
    return '.'.join(filename.split('.')[:-1])


def targz_corename(filename):
    return '.'.join(filename.split('.')[:-2])


def remove_bizarre_chars(strng):
    return re.sub(r'[^a-zA-Z0-9_-]', r'_', strng)


def treat_targz(path_prefix, targz_name):
    targz_path = os.path.join(path_prefix, targz_name)
    print("Looking at %s in %s" % (targz_path, path_prefix))
    targz = tarfile.open(targz_path, "r:gz")
    pdf = None
    nlm = None

    try:
        for item in targz:
            if PDF_RE.match(item.name):
                pdf = item.name
            if NLM_RE.match(item.name):
                nlm = item.name

        if nlm is not None and pdf is not None:

            tar_corename = targz_corename(targz_name)
            new_dirname = remove_bizarre_chars(tar_corename)

            old_name = os.path.join(path_prefix, tar_corename)
            new_name = os.path.join(path_prefix, new_dirname)

            if os.path.exists(new_name):
                print("Path already exists: %s. No need to proceed" % new_name)
                return

            print("Extracting %s and %s to %s" % (pdf, nlm, path_prefix))
            targz.extract(pdf, path_prefix)
            targz.extract(nlm, path_prefix)

            if old_name != new_name:
                print("Rename %s to %s" % (old_name, new_name))
                try:
                    shutil.move(old_name, new_name)
                except shutil.Error, e:
                    print("Can't rename %s to %s: %s" % (old_name, new_name, str(e)))
                    return

            nlm = nlm.split('/')[-1]
            pdf = pdf.split('/')[-1]
            nlm_path = os.path.join(new_name, nlm)
            pdf_path = os.path.join(new_name, pdf)
            print(nlm_path, pdf_path)
            try:
                nlm_id = get_nlm_id(nlm_path)
            except Exception, e:
                print("Unable to read %s" % nlm_path)
                return

            print("Read nlm id: %s" % nlm_id)
            new_nlm_name = "%s.nxml" % nlm_id
            new_pdf_name = "%s.pdf" % nlm_id
            print("Rename %s to %s" % (nlm_path, new_nlm_name))
            print("Rename %s to %s" % (pdf_path, new_pdf_name))
            try:
                shutil.move(
                    nlm_path,
                    os.path.join(
                        path_prefix,
                        new_dirname,
                        new_nlm_name))
            except shutil.Error, e:
                print("Problem with renaming %s: %s" % (nlm_path, str(e)))

            try:
                shutil.move(
                    pdf_path,
                    os.path.join(
                        path_prefix,
                        new_dirname,
                        new_pdf_name))
            except shutil.Error, e:
                print("Problem with renaming %s: %s" % (nlm_path, str(e)))
        else:
            print("Either pdf or nxml missing in %s" % targz_path)

    finally:
        print("Removing tar %s" % targz_path)
        targz.close()
        os.remove(targz_path)


def clean(path_prefix):
    specific_dir = "%s/pub/pmc/%s" % (PMC, path_prefix)
    if os.path.exists(specific_dir):
        try:
            os.remove("%s/.listing" % specific_dir)
        except:
            pass
        files = os.listdir(specific_dir)
        targzs = [f for f in files if TARGZ_RE.match(f)]
        if len(targzs) == 0:
            print("Nothing to do in %s" % specific_dir)
        else:
            print("Doing some job in %s" % specific_dir)
            map(partial(treat_targz, message_q, specific_dir), targzs)
    else:
        print("Directory doesn't exist: %s" % specific_dir)

if __name__ == "__main__":
    path_prefixes = ["%02x/%02x" % (x, y) for x in xrange(0, 256) for y in xrange(0, 256)]
    pool = Pool(processes=NUMBER_OF_PROCESSES)
    pool.map(clean, path_prefixes)
    pool.close()
    pool.join()
    # for pref in path_prefixes:
    #     clean(pref)
