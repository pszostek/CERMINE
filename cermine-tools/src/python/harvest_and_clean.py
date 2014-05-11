#!/usr/bin/env python

from multiprocessing import Pool
from subprocess import call
import ftplib as ftp
import os
import threading
import Queue
import re
import libxml2
import logging
from clean import treat_targz


PMC = "ftp.ncbi.nlm.nih.gov"
TARGZ_RE = re.compile(r".*\.tar\.gz")
PDF_RE = re.compile(r".*\.pdf")
NLM_RE = re.compile(r".*\.nxml")
NUMBER_OF_CORES = 48

NUMBER_OF_PROCESSES = 47

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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



class ListThread(threading.Thread):

    def __init__(self, file_q, local_dirs, prefixes_q):
        super(ListThread, self).__init__()
        self.file_q = file_q
        self._local_dirs = local_dirs
        self.prefixes_q = prefixes_q
        self.stoprequest = threading.Event()
        self._conn = ftp.FTP(PMC, "anonymous", "anonymous")
        self._conn.cwd("pub/pmc")
        

    def is_already_there(self, path):
        parts = path.split('/')
        prefix = '/'.join(parts[:-1])
        targz_name = parts[-1]
        targz_corename = '.'.join(targz_name.split('.')[:-2])
        nonbizarre = remove_bizarre_chars(targz_corename)
        if os.path.join(prefix, nonbizarre) in self._local_dirs:
            #print("File %s is already there" % path)
            return True
        #print("File %s in not there" % path)
        return False

    def is_targz(self, path):
        return TARGZ_RE.match(path)

    def run(self):
        if not self.stoprequest.isSet():
            while True:
                try:
                    prefix = self.prefixes_q.get(block=True, timeout=3600)
                except Queue.Empty:
                    break
                remotes = self.get_remote_files(prefix)
                print("%d files in %s prefix" % (len(remotes), prefix))
                for remote in remotes:
                    if self.is_targz(remote):
                        if not self.is_already_there(remote):
                            print("Missing %s" % remote)
                            file_q.put(remote)
                        else:
                            print("Not missing %s" % remote)

    def get_remote_files(self, prefix):
        fl = self._conn.nlst(prefix)
     #   self._conn.close()
        return fl


class DownloadThread(threading.Thread):

    def __init__(self, file_q):
        super(DownloadThread, self).__init__()
        self.file_q = file_q
        self.stoprequest = threading.Event()

    def run(self):
        while not self.stoprequest.isSet():
            filename = self.file_q.get(block=True, timeout=3600)
            self.download_file(filename)
            path_prefix = os.path.join(PMC, "pub", "pmc", os.path.dirname(filename))
            targz_name = os.path.basename(filename)
            treat_targz(path_prefix, targz_name)

    def download_file(self, filename):
        cmd = "wget --mirror 'ftp://%s/pub/pmc/%s'" % (PMC, filename)
        print(cmd)
        call(cmd, shell=True)
    #    shutil.move(os.path.basename(filename), "%s/pub/pmc/%s" % (PMC, filename))

def _get_local_dirs_from_prefix(prefix):
    paths = []
    try:
        files = os.listdir("ftp.ncbi.nlm.nih.gov/pub/pmc/%s" % prefix)
        for file_ in files:
            paths.append("%s/%s" % (prefix, file_))
        return paths
    except OSError:
        return []


def get_local_dirs(prefixes):
    pool = Pool(processes=15)
    local_files = pool.map(_get_local_dirs_from_prefix, prefixes)
    local_files = [item for sublist in local_files for item in sublist]
    local_files = set(local_files)
    pool.close()
    pool.join()
    return local_files


# def get_local_files_from_file(path):
#     fle = open(path, 'r')
#     files = set([line.strip() for line in fle.readlines()])
#     fle.close()


if __name__ == "__main__":
    file_q = Queue.Queue()
    prefixes = ["%02x/%02x" % (x, y) for x in xrange(0, 256) for y in xrange(0, 256)]
    #local_files = get_local_files_from_file('./harvest2.output')
    local_dirs = get_local_dirs(prefixes)
    print("local dirs %d" % len(local_dirs))
###
    p_len = len(prefixes)
    assert p_len % 4 == 0
    prefixes_q = Queue.Queue()
    for prefix in prefixes:
        prefixes_q.put(prefix)
    file_q = Queue.Queue()

    list_pool = []
    for _ in xrange(0, 8):
        list_pool.append(ListThread(file_q=file_q, local_dirs=local_dirs, prefixes_q=prefixes_q))
    for thread in list_pool:
        thread.start()
    download_pool = [DownloadThread(file_q=file_q) for _ in xrange(0, NUMBER_OF_CORES)]
    for thread in download_pool:
        thread.start()
