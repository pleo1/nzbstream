import logging

try:
    from pymediainfo import MediaInfo
except:
    MediaInfo = None

import pdb

log = logging.getLogger('nzbstream.mediafile')

class InvalidMediaFile(Exception):
    pass

class MediaFile(object):
    EXTENSIONS = ['mkv', 'avi', 'mpeg', 'mpg', 'mp4']

    def __init__(self, header):
        ext = header.filename.rsplit('.', 1)[-1]
        if ext.lower() not in self.EXTENSIONS:
            log.debug("Invalid file type...skipping")
            raise InvalidMediaFile("Invalid file type")

        # TODO: Verify file size

        # Create the file
        self.header     = header
        self.filename   = header.filename
        self.path       = header.filename   # TODO: Support output directory
        self.file_size  = header.file_size
        self.duration   = 0
        self._fd        = open(header.filename, 'wb')

    def write(self, data):
        if not self._fd:
            return
        log.debug("Writing %d bytes" % len(data))
        self._fd.write(data)

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
        return self.file_size/(self.duration/1000.0)