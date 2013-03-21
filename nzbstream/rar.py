import logging
import rarfile
import re
import time

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

import pdb

from mediafile import MediaFile

RAR_FIRST_RE = re.compile(r"(\.001|\.part0*1\.rar|^((?!part\d*\.rar$).)*\.rar)$")
#RAR_RE       = re.compile(r"(\.[\d]+|\.part[\d]+\.rar|\.rar)$")
RAR_RE    = re.compile(r'\.(rar|r\d\d|[\d]+)$')
FILE_EXT = ['mkv', 'avi', 'mpeg', 'mpg', 'mp4']

class RarSet(object):
    def __init__(self, nzb):
        self.first_archive  = None
        self.archive_name   = None
        self.rars           = []

        self._offset            = 0             # Offset from beginning of extracted file
        self._segment           = None          # Current segments
        self._header            = None          # Current header
        self._header_read_size  = 0             # Number of bytes read from current archive
        self._buf               = StringIO()    # Current buffer
        self._fd                = None          # File handle for extracted file
        self._rf                = None          # RarFile instance
        self._headers           = []            # List of all headers found in total archive (all volumes)
        self._file_name         = ""            # Extracted file name

        self._parse(nzb)

    def __repr__(self):
        return "<RarSet: %s>" % self.archive_name

    def __str__(self):
        return self.archive_name

    def _sort_file(self, f):
        subject = f.subject
        if '"' in subject:
            tokens = subject.split('"')
            if len(tokens) == 3:
                subject = tokens[1]
        f.filename = subject
        return subject

    def _parse(self, nzb):
        nzb     = sorted(nzb, key=self._sort_file)
        names   = {}
        for file in nzb:
            if RAR_RE.search(file.filename):
                if self.first_archive is None and (file.filename.find('.rar') > 0):
                    self.rars.insert(0, file)
                    self.first_archive = file
                else:
                    self.rars.append(file)

                name = file.filename.rsplit('.', 1)[0]
                if names.has_key(name):
                    names[name] += 1
                else:
                    names[name] = 1

        max_count = 0
        for name, count in names.items():
            if count > max_count:
                max_count = count
                self.archive_name = name

        # TODO: This doesn't work with "xxx.part01.rar" style naming.
        for rar in self.rars:
            if rar.filename.rsplit('.', 1)[0] != self.archive_name:
                self.rars.remove(rar)

    def _get_segment(self):
        for rar in self.rars:
            for segment in rar.segments:
                yield segment

    def reset(self):
        if self._fd:
            self._fd.close()

        self._offset            = 0
        self._segment           = None
        self._header            = None
        self._header_read_size       = 0
        self._buf               = StringIO()
        self._fd                = None
        self._file_name         = ""

    def read(self, data):
        # Add the data into the buffer
        data_len = len(data)
        pos = self._buf.tell()
        self._buf.write(data)
        self._buf.seek(pos)

        if self._rf is None:
            id = self._buf.read(len(rarfile.RAR_ID))
            if id != rarfile.RAR_ID:
                logging.error("Not a Rar file...resetting")
                self.reset()
                return

            logging.debug("Found Rar ID")
            self._rf = rarfile.RarFile("", partial_ok=True, stream=True, parse=False)

        if not self._header or (self._header and self._header_read_size == self._header.add_size):
            logging.debug("Looking for headers")
            headers = []
            while True:
                pos = self._buf.tell()
                try:
                    header = self._rf._parse_header(self._buf)
                    if not header:
                        self._buf.seek(pos)
                        break
                    logging.debug("Found header type %x" % header.type)
                    headers.append(header)
                except Exception, e:
                    logging.error(e)
                    self._buf.seek(pos)
                    break

            if len(headers) == 0:
                logging.debug("No headers found")
                return

            logging.debug("Found %d headers" % len(headers))
            self._headers.extend(headers)

            for header in headers:
                if header.type == rarfile.RAR_BLOCK_FILE:
                    logging.debug("Found file in Rar: %s" % header.filename)
                    if not self._fd:
                        try:
                            self._fd = MediaFile(header)
                        except:
                            logging.info("Invalid file type...skipping")
                            return

                        self._file_name = header.filename
                        
                        #ext = header.filename.rsplit('.', 1)[-1]
                        #if ext.lower() not in FILE_EXT:
                        #    logging.info("Invalid file type...skipping")
                        #    return

                        #if header.compress_type != rarfile.RAR_M0:
                        #    logging.error("Rar is not in uncompressed format...cannot continue")
                        #    return False
                        #else:
                        #    logging.debug("Rar is in uncompressed format...good to go!")

                        #self._file_name = header.filename
                        #self._fd        = open(header.filename, "wb+")

                    if header.filename != self._file_name:
                        logging.error("Filename mismatch: %s != %s", header.filename, self._file_name)
                        return

                    if self._fd.tell() > 0:
                        logging.debug("Continuing file %s" % header.filename)

                    self._header = header
                    self._header_read_size = 0

        if self._fd and self._header:
            count = self._header.add_size-self._header_read_size
            if count > 0:
                pos = self._buf.tell()
                self._fd.write(self._buf.read(count))
                written = self._buf.tell()-pos

                logging.debug("Wrote %d of %d bytes into file" % (written, data_len))
                self._offset      += written
                self._header_read_size += written

            pos, size = self._fd.tell(), self._header.file_size
            logging.debug("Written %d/%d" % (pos, size))
            #logging.info("Progress: %0.2f%%" % ((float(pos)/size)*100))
            if self._fd.tell() == self._header.file_size:
                logging.info("File completed!")
                self._fd.close()
                
        # read remaining buffer, if any
        data = self._buf.read()
        #if data:
        #    pdb.set_trace()
        self._buf.seek(0)
        self._buf.truncate()
        self._buf.write(data)

        if self._fd and self._header:
            if not self._fd.closed:
                return self._fd.tell(), self._header.file_size
            return self._header.file_size, self._header.file_size
