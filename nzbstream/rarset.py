import logging
import rarspec
import re
import time

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

from rarfile import RarFile

import pdb

RAR_FIRST_RE = re.compile(r"(\.001|\.part0*1\.rar|^((?!part\d*\.rar$).)*\.rar)$")
RAR_RE       = re.compile(r'\.(rar|r\d\d|[\d]+)$')

log = logging.getLogger('nzbstream.rarset')

RAR_HEADER_NAMES = {
    0x72: "RAR_BLOCK_MARK",
    0x73: "RAR_BLOCK_MAIN",
    0x74: "RAR_BLOCK_FILE",
    0x75: "RAR_BLOCK_OLD_COMMENT",
    0x76: "RAR_BLOCK_OLD_EXTRA",
    0x77: "RAR_BLOCK_OLD_SUB",
    0x78: "RAR_BLOCK_OLD_RECOVERY",
    0x79: "RAR_BLOCK_OLD_AUTH",
    0x7a: "RAR_BLOCK_SUB",
    0x7b: "RAR_BLOCK_ENDARC"
}

NUM_RE = re.compile('([0-9]+)')

class RarSet(object):
    """
    A class for managing a set of rarchives, as defined:

        name.rar name.r01 name.r02 name.r03 ... name.rN ]- rarset
        |      | |      |                       |      |
        +------+ +------+           ...         +------+
        rarchive rarchive                       rarchive



    The individular rarchives should be downloaded and parsed in the correct
    order.  The RarSet class manages reading headers and writting the rarchive
    file contents into the correct buffer.
        
                           rarchive                     rarchive
        +-----------------------------------------+--------------------------->
        |                                         |                 
        .......................................................................
        |      |      |                    |      |      |      |             |
        +------+------+--------------------+------+------+------+-------------+
         header header     file contents    header header header file contents

    """
    def __init__(self, nzb):
        self.name           = None  # Rarset name (without extension)
        self.first_rarchive = None  # Firs rarchive in the rarset
        self.rarchives      = []    # List of rarchives in rarset
        self.files          = {}    # Files contained in the entire rarset
        self.current_file   = None  # Current file in the rarset

        self._offset            = 0             # Offset from beginning of extracted file
        self._segment           = None          # Current segments
        self._header            = None          # Current header
        self._header_read_size  = 0             # Number of bytes read from current archive
        self._buf               = StringIO()    # Current buffer
        self._fd                = None          # File handle for extracted file
        self._rs                = None          # RarFile instance
        self._headers           = []            # List of all headers found in total archive (all volumes)
        self._file_name         = ""            # Extracted file name

        self._rs = rarspec.RarSpec("", partial_ok=True, stream=True, parse=False)

        self._parse(nzb)

    def __repr__(self):
        return "<RarSet: %s>" % self.name

    def __str__(self):
        return self.name

    def _sort_file(self, f):
        return [ int(c) if c.isdigit() else c for c in NUM_RE.split(f.filename) ]

    def _parse(self, nzb):
        nzb     = sorted(nzb, key=self._sort_file)
        names   = {}

        for file in nzb:
            if RAR_RE.search(file.filename):
                self.rarchives.append(file)

        self.name, length = self._get_common_name(self.rarchives[0].filename, self.rarchives[1].filename)
        log.debug("Found archive name: %s" % self.name)

        for rar in self.rarchives:
            if not self._check_name(rar.filename, length):
                if hasattr(rar, 'keep') and rar.keep:
                    continue
                log.debug("Removing: %s" % rar.filename)
                self.rarchives.remove(rar)

        rar_count = 0
        rar_pos   = 0
        for i, rar in enumerate(self.rarchives):
            if rar.filename.split('.')[-1] == "rar":
                rar_count += 1
                rar_pos = i

        if rar_count == 1:
            if self.rarchives[rar_pos].filename.replace(self.name, '') == ".rar":
                self.rarchives.insert(0, self.rarchives.pop(rar_pos))

        log.debug("Rarchives:")
        for rar in self.rarchives:
            log.debug("  %s" % rar.filename)

    def _check_name(self, name, length):
        bits = name.split('.')
        if len(bits) != length+1:
            return False
        return True

    def _get_common_name(self, f1, f2):
        l1      = f1.split('.')
        l2      = f2.split('.')
        count   = 0

        for i, v in enumerate(l1):
            if l2[i] == v:
                count += 1
            else:
                break

        return ".".join(l1[:count]), count

    def _get_segment(self):
        for rar in self.rarchives:
            for segment in rar.segments:
                yield segment

    def reset(self):
        if self._fd:
            self._fd.close()

        self._offset            = 0
        self._segment           = None
        self._header            = None
        self._header_read_size  = 0
        self._buf               = StringIO()
        self._fd                = None
        self._file_name         = ""

    def _find_file(self):
        if not self.current_file or not self.current_file._header:
            log.debug("Looking for headers")
            headers = []
            while True:
                pos = self._buf.tell()
                try:
                    header = self._rs._parse_header(self._buf)
                    if not header:
                        self._buf.seek(pos)
                        break
                    log.debug("Found header type: %s" % RAR_HEADER_NAMES[header.type])
                    headers.append(header)
                    if header.type == rarspec.RAR_BLOCK_FILE:
                        if not self.files.has_key(header.filename):
                            log.debug("Creating new RarFile for %s" % header.filename)
                            self.files[header.filename] = RarFile(header)
                        else:
                            log.debug("Continuing RarFile for %s" % header.filename)
                            self.files[header.filename].add_header(header)

                        self.current_file = self.files[header.filename]
                        break
                except Exception, e:
                    log.error(e)
                    self._buf.seek(pos)
                    break

            log.debug("Found %d headers" % len(headers))
            self._headers.append(headers)

        return self.current_file

    def read(self, data):
        # Add the data into the buffer
        data_len = len(data)
        pos = self._buf.tell()
        self._buf.write(data)
        self._buf.seek(pos)

        self.current_file = self._find_file()
        if self.current_file:
            bytes_written = self.current_file.write(self._buf)
            if self.current_file.complete:
                log.debug("File is complete")
                self.current_file = None

        # read remaining buffer, if any
        data = self._buf.read()
        self._buf.seek(0)
        self._buf.truncate()
        self._buf.write(data)
