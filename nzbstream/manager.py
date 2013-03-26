import hashlib
import logging
import pynzb
import re
import sys
import urllib

from nzbstream import nntp, rarset, par2

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

log = logging.getLogger('nzbstream.manager')

FILE_HASH16K_LENGTH = 16 * 1024 # 16 KB, in bytes
PAR_RE = re.compile(r'(vol[\d\+]+).par2')
BITRATE_STREAM_MULT = 2

def get_filename(subject):
    if '"' in subject:
        tokens = subject.split('"')
        if len(tokens) == 3:
            subject = tokens[1]
    return subject

class Manager(object):
    def __init__(self, nzb_path, nntp_kwargs, max_bitrate=None, do_verify=True):
        self.nzb_path     = nzb_path
        self.nntp_kwargs  = nntp_kwargs
        self.max_bitrate  = max_bitrate
        self.do_verify    = do_verify
        self.current_file = None

        self.nzb_file    = None
        self.nzb         = None
        self.rs          = None
        self.server      = None
        self.segments    = {}
        self._segnum     = -1
        self._segment    = None
        self._segcount   = 0

    def log(self, msg, lvl=0):
        log.info(msg.replace('\n', ' '))
        sys.stdout.write("%s%s" % ("  "*lvl, msg))
        sys.stdout.flush()
    
    def logn(self, msg, lvl=0):
        self.log(msg+"\n", lvl)

    def next_segment(self):
        segnum  = self._segnum+1
        segment = self.segments.get(segnum)
        if not segment:
            self.logn("[Error] Couldn't find segment %d" % segnum, 1)
            return False

        log.debug("Grabbing segment %d" % segnum)
        self.server.add_segment(segment, segnum)
        while True:
            # Loop until the server has downloaded the segment
            data = self.server.get_segment(segnum, timeout=2)
            if not data:
                continue
            self._segment = data
            self._segnum  = segnum
            break

        log.debug("Got segment %d" % segnum)
        return self._segment

    def initialize(self):
        """
        Opens necessary files and determines if and how to resume a previous
        stream.  If this returns ``False``, no further attempt should be made
        to download this file.
        """
        self.logn("Initializing")
        
        self.log("Opening NZB...", 1)
        try:
            #self.nzb_file = open(self.nzb_path)
            self.nzb_file = urllib.urlopen(self.nzb_path)
        except Exception, e:
            self.logn("\n[Error] Could not open NZB: %s" % e, 1)
            return False
        self.logn("OK")

        self.log("Parsing NZB...", 1)
        try:
            self.nzb = pynzb.nzb_parser.parse(self.nzb_file.read())
        except Exception, e:
            self.logn("\n[Error] Could not parse NZB: %s" % e, 1)
            return False
        self.logn("OK")

        self.logn("Connecting to server (%d threads)" % (self.nntp_kwargs.get('threads',1)), 1)
        self.server = nntp.NNTP(**self.nntp_kwargs)


        # Some posters rename the rarchives after they have been created and the
        # NZB will actually refer to the renamed name.  For example:
        #
        #   filename.rar -> 2823e14c6a6d28b9bd26252d5.21
        #   filename.r01 -> 2823e14c6a6d28b9bd26252d5.43
        #   filename.r02 -> 2823e14c6a6d28b9bd26252d5.0
        #   filename.r03 -> 2823e14c6a6d28b9bd26252d5.10
        #   ...
        #
        # The proper file names are those of the left and the renamed versions
        # on the right and the NZB will refer to the names on the right.  This
        # is problematic, however, because sorting on the renamed version of the
        # name will put the rarchives in the wrong order; i.e we'll grab
        # "filename.r02" before "filename.rar".  To remedy this situation, we
        # instead grab the parity file, "filename.par2".  This partity file
        # contains the proper mapping between names via a 16kB MD5 hash.  In
        # order to map one name to the other, we need to grab the first 16kB of
        # every file and compute it's MD5 hash.  We then parse the parity file
        # and rename the files by matching the MD5 hashes in the parity file
        # with those that we've calculated.
        self.logn("Determining proper filenames and ordering", 1)
        self.log("Grabbing segments for MD5 hashes...", 2)
        for i, f in enumerate(self.nzb):
            self.server.add_segment(f.segments[0], i)
        self.logn("done")

        self.log("Generating MD5 hashes...", 2)
        hash_map = {}
        file_map = {}
        par_files = []
        for i, f in enumerate(self.nzb):
            while True:
                data = self.server.get_segment(i, 2)
                if not data:
                    continue

                filename = get_filename(f.subject)
                f.filename = filename
                file_map[filename] = data
                hash_map[filename] = hashlib.md5(data[:FILE_HASH16K_LENGTH]).digest()
                if filename.endswith('.par2') and not PAR_RE.search(filename):
                    log.debug("Found par2: %s" % filename)
                    par_files.append(filename)
                break
        self.logn('done')
        
        if par_files:
            for par_file in par_files:
                self.logn("Found par2 file: %s" % par_file, 2)
                pf = par2.Par2File(StringIO(file_map[par_file]))
                for packet in pf.packets:
                    if packet.fmt == par2.FILE_DESCRIPTION_PACKET:
                        for name, hash in hash_map.items():
                            if packet.file_hash16k == hash:
                                for f in self.nzb:
                                    if f.filename == name:
                                        f.filename = packet.name
                                        f.keep = True
                                        log.debug("Mapped %s -> %s" % (name, packet.name))
                                        break
                                break

        self.logn("Looking for rar archives in NZB", 1)
        self.rs = rarset.RarSet(self.nzb)

        if len(self.rs.rarchives) == 0:
            self.logn("[Error] No rar archives found", 1)
            return False

        # TODO: Read in possible resume file, and continue from a particular
        #       segment rather than from the beginning.
        num_segments = 0
        for rarfile in self.rs.rarchives:
            for segment in rarfile.segments:
                #server.add_segment(segment, num_segments)
                self.segments[num_segments] = segment
                num_segments += 1
        self._segcount = num_segments
        self.logn("Found %d rar files and %d segments" % (len(self.rs.rarchives), num_segments), 2)

        self.logn("Initialization OK", 1)
        return True

    def verify(self):
        """
        Attempts to verify that this file is capable of being streamed.  The
        following items are always checked:

            *   Rar files do not use compression.
            *   Rar files contain a media file

        These checks are optional:

            *   If ``do_verify`` is True, a check will be made to ensure that
                every segment of every rar file is available on the news server.
            *   Media file bitrate is equal to or below the user-defined
                ``max_bitrate``.
        """
        self.logn("Verifying stream")
        
        # Let's first check that the rar files do not use compression
        self.logn("Looking for rar header", 1)
        while True:
            data = self.next_segment()

            # Parse the data
            self.rs.read(data)
            if self.rs.current_file:
                if self.rs.current_file != self.current_file:
                    self.current_file = self.rs.current_file
                    self.logn("Found file: %s" % self.rs.current_file.filename, 2)

                    if self.current_file.is_media():
                        self.logn("File is a valid media type")
                    else:
                        self.logn("File is not a valid media type")
                        continue

                # See if we need to check the bitrate
                if self.max_bitrate is not None:
                    
                    self.logn("Checking bitrate", 2)
                    bitrate = self.current_file.get_bitrate()
                    while not bitrate:
                        segment = self.next_segment()
                        if not segment:
                            self.logn("[Error] Couldn't find segment", 2)
                            return False

                        self.rs.read(data)

                        bitrate = self.rs.current_file.get_bitrate()

                    self.logn("Bitrate is %s" % nntp.sizeof_fmt(bitrate), 3)
                    if self.max_bitrate < bitrate:
                        self.logn("[Error] Bitrate exceeds user-defined max of %s" % nntp.sizeof_fmt(self.max_bitrate), 3)
                        return False
                    self.logn("Bitrate is <= %s" % nntp.sizeof_fmt(self.max_bitrate), 3)
                    break
            break

        if self.do_verify:
            self.logn('Verifying rar segments...', 1)
            # TODO: Verify segments are available on the server
            pass

        return True

    def stream(self):
        self.logn("Starting stream")
        segnum  = self._segnum+1
        bitrate = None

        self.logn("Queuing segments", 1)
        while segnum < self._segcount:
            segment = self.segments[segnum]
            self.server.add_segment(segment, segnum)
            segnum += 1

        while self._segnum < self._segcount:
            segnum  = self._segnum+1
            while True:
                data = self.server.get_segment(segnum, 2)
                if not data:
                    self.display_progress()
                    continue

                ret = self.rs.read(data)
                if not bitrate:
                    bitrate = self.current_file.get_bitrate()
                    if bitrate:
                        self.logn("Bitrate is %s" % nntp.sizeof_fmt(bitrate), 2)
                        self.logn("Setting download throttle to ~%s" % nntp.sizeof_fmt(bitrate*BITRATE_STREAM_MULT), 2)
                        self.server.set_throttle(bitrate*BITRATE_STREAM_MULT)

                self.display_progress()

                if self.current_file.complete:
                    self.logn("\nStream complete!")
                    return
                break

            self._segnum = segnum

    def display_progress(self):
        sys.stdout.write("\rProgress: %0.2f%%, Rate: %12s" % ((self.current_file.get_progress()*100), self.server.get_speed(True)))
        sys.stdout.flush()
