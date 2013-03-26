import logging
import os

try:
    from pymediainfo import MediaInfo
except:
    MediaInfo = None

import pdb

log = logging.getLogger('nzbstream.rarfile')

class RarFile(object):
    EXTENSIONS = ['mkv', 'avi', 'mpeg', 'mpg', 'mp4']

    def __init__(self, header):
        # Create the file
        self.filename   = header.filename
        self.path       = header.filename   # TODO: Support output directory
        self.file_size  = header.file_size
        self.duration   = 0
        self.complete   = False
        self.realname   = None

        self._header    = header                # Current header
        self._headers   = []                    # All headers seen so far
        self._fd        = open(self.path, 'wb') # The actual file on disk

        # Number of bytes written for the current header.  Once this value reaches
        # header.add_size, we must be given the next header before attempting to
        # write more data.
        self._written       = 0                     
        self._total_written = 0

        self.add_header(header)

    def add_header(self, header):
        if header.filename != self.filename:
            # Invalid header
            return

        for hdr in self._headers:
            if hdr.header_crc == header.header_crc:
                log.debug("Duplicate header crc")
                return

        self._headers.append(header)
        self._header    = header
        self._written   = 0

    def get_progress(self):
        return self._total_written/float(self.file_size)

    def write(self, data):
        if not self._fd or not self._header or self.complete:
            return

        pos = data.tell()
        data.seek(0, os.SEEK_END)
        total_bytes = data.tell()-pos
        data.seek(pos)

        bytes_to_write  = self._header.add_size-self._written
        pos             = self._fd.tell()

        self._fd.write(data.read(bytes_to_write))

        bytes_written        = self._fd.tell()-pos
        self._written       += bytes_written
        self._total_written += bytes_written
        
        log.debug("Wrote %d bytes of %d bytes" % (bytes_written, total_bytes))
        if self._written == self._header.add_size:
            if self._total_written == self.file_size:
                log.debug("File complete!")
                self.complete = True
            else:
                log.debug("Waiting for next rarchive")
            
            log.debug("File is %0.2f%% complete" % (float(self._total_written)/self.file_size*100))
            self._header = None
        else:
            log.debug("Wrote %d/%d bytes for rarchive" % (self._written, self._header.add_size))

        return bytes_written

    def tell(self):
        if not self._fd:
            return 0
        return self._fd.tell()

    def close(self):
        if not self._fd:
            return
        self._fd.close()

    def is_closed(self):
        return self._fd is not None and self._fd.closed
    closed = property(is_closed)

    def is_media(self):
        ext = self.filename.rsplit('.', 1)[-1]
        if ext.lower() not in self.EXTENSIONS:
            log.debug("File is not a media file")
            return False
        return True

    def get_duration(self):
        if MediaInfo is not None:
            for track in MediaInfo.parse(self.path).tracks:
                if track.track_type == 'Video':
                    log.debug("Found video track with duration %d" % track.duration)
                    self.duration = track.duration
        return self.duration

    def get_bitrate(self):
        """
        Returns the bitrate in bits per second of the media file.  Rather than using the
        bitrate supplied by MediaInfo, it is instead recalclated by taking the full file
        size, as reported by the rar headers (very accurate), and the duration from MediaInfo
        (which is usually accurate).  As a result, the bitrate is a combination of audio, video
        and an other embedded files such as subtitles.

        Returns 0 if no bitrate can be determined.
        """
        duration = self.get_duration()
        if duration == 0:
            return 0
        return self.file_size/(self.duration/1000.0)*8.0
