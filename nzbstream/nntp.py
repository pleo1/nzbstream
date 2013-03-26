from nzbverify import nntp
from nntplib import NNTPError, NNTPPermanentError, NNTPTemporaryError

import logging
import Queue
import re
import threading
import time
import _yenc

YSPLIT_RE = re.compile(r'([a-zA-Z0-9]+)=')
gUTF      = True
TIME_SEP  = 0.5

log = logging.getLogger('nzbstream.nntp')

def sizeof_fmt(num, bytes=False):
    if bytes:
        num *= 8
    for x in ['Bps','Kbps','Mbps','Gbps','Tbps']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def name_fixer(p):
    """ Return UTF-8 encoded string, if appropriate for the platform """

    if gUTF and p:
        return p.decode('Latin-1', 'replace').encode('utf-8', 'replace').replace('?', '_')
    else:
        return p

def ySplit(line, splits = None):
    fields = {}

    if splits:
        parts = YSPLIT_RE.split(line, splits)[1:]
    else:
        parts = YSPLIT_RE.split(line)[1:]

    if len(parts) % 2:
        return fields

    for i in range(0, len(parts), 2):
        key, value = parts[i], parts[i+1]
        fields[key] = value.strip()

    return fields

def yCheck(data):
    ybegin = None
    ypart = None
    yend = None

    ## Check head
    for i in xrange(min(40, len(data))):
        try:
            if data[i].startswith('=ybegin '):
                splits = 3
                if data[i].find(' part=') > 0:
                    splits += 1
                if data[i].find(' total=') > 0:
                    splits += 1

                ybegin = ySplit(data[i], splits)

                if data[i+1].startswith('=ypart '):
                    ypart = ySplit(data[i+1])
                    data = data[i+2:]
                    break
                else:
                    data = data[i+1:]
                    break
        except IndexError:
            break

    ## Check tail
    for i in xrange(-1, -11, -1):
        try:
            if data[i].startswith('=yend '):
                yend = ySplit(data[i])
                data = data[:i]
                break
        except IndexError:
            break

    return ((ybegin, ypart, yend), data)

def decode(data):
    yenc, data = yCheck(data)
    ybegin, ypart, yend = yenc
    decoded_data = None

    #Deal with non-yencoded posts
    # TODO:
        
    #Deal with yenc encoded posts
    if (ybegin and yend):
        if 'name' in ybegin:
            filename = name_fixer(ybegin['name'])
        _type = 'yenc'

        # Decode data
        decoded_data, crc = _yenc.decode_string(''.join(data))[:2]
        partcrc = '%08X' % ((crc ^ -1) & 2**32L - 1)

        if ypart:
            crcname = 'pcrc32'
        else:
            crcname = 'crc32'

        if crcname in yend:
            _partcrc = '0' * (8 - len(yend[crcname])) + yend[crcname].upper()
        else:
            # Corrupt header...
            _partcrc = None
        if not (_partcrc == partcrc):
            log.error("CRC Error")

        return decoded_data

class NNTPThread(threading.Thread):
    """
    A thread for consuming message ids and decoding articles.
    """
    daemon = True

    def __init__(self, name, owner, nntp_kwargs):
        self.owner       = owner
        self.msg_ids     = owner._msg_ids
        self.articles    = owner._articles
        self.nntp_kwargs = nntp_kwargs
        self.conn        = None
        self._halt       = False

        super(NNTPThread, self).__init__(name=name)

    def quit(self):
        log.debug("Thead quitting")
        self._halt = True
        self.msg_ids.put((-1, None))

    def get_conn(self):
        if not self.conn:
            log.debug("Connecting")
            self.conn = nntp.NNTP(**self.nntp_kwargs)
        return self.conn

    def close_conn(self):
        log.debug("Disconnecting")
        try:
            self.conn.quit()
        except:
            pass
        self.conn = None

    def run(self):
        log.debug("Thread starting")
        while not self._halt:
            order, message_id = None, None
            try:
                conn = self.get_conn()

                order, message_id = self.msg_ids.get()
                if order == -1:
                    self.close_conn()
                    break

                time.sleep(self.owner._delay)

                start = time.time()
                article = conn.article(message_id)
                stop = time.time()

                article = decode(article[3])
                self.articles[order] = (article, start, stop)
                self.owner.add_bytes(len(article))
                self.msg_ids.task_done()
                log.debug("Segment %d downloaded" % order)

                order, message_id = None, None
            except Queue.Empty:
                pass
            except NNTPPermanentError, e:
                # Dead
                log.error("Permanent NNTP error; quitting")
                log.error(e)
                break
            except NNTPTemporaryError, e:
                if e.response.startswith('430'):
                    # No such article...
                    log.error("No such article: %s" % e)
                    # This is a fatal error
                    # TODO: Signal fatal error
            except Exception, e:
                log.error('%s: %s' %(type(e), e))
                self.close_conn()
            finally:
                if order and message_id:
                    # Put the message id back into the queue
                    log.debug("Putting message back in queue: (%s, %s)" % (order, message_id))
                    self.msg_ids.put((order, message_id))

        log.debug("Thead quit")

class NNTP(object):
    def __init__(self, host, port, user=None, password=None, use_ssl=None,
                 timeout=10, threads=1):
        
        self.host       = host
        self.port       = port
        self.user       = user
        self.password   = password
        self.use_ssl    = use_ssl
        self.timeout    = timeout
        self.threads    = threads
        self._pool      = []
        self._bytes     = 0
        self._timer     = time.time()
        self._speed     = 0

        self._msg_ids   = Queue.PriorityQueue()
        self._articles  = {}
        self._lock      = threading.Lock()

        self._throttle  = 0
        self._delay     = 0

        self._total_bytes = 0
        self._start_time  = time.time()

        self.connect()

    def add_segment(self, segment, order=1):
        msgid = "<%s>" % segment.message_id
        self._msg_ids.put((order, msgid))

    def set_throttle(self, bps):
        """
        Throttle download speed, in bits per second.
        """
        self._throttle = bps/8.0 # Stored as Bytes/sec

    def add_bytes(self, bytes):
        self._lock.acquire()
        now = time.time()
        dt  = now-self._timer
        if dt >= TIME_SEP:
            speed   = self.get_speed()          # Speed in Bytes/sec
            dt      = now-self._start_time

            self._total_bytes += self._bytes
            self._bytes        = 0
            self._timer        = now

            # Here is the throttling code.  It is pretty dirty, but works more or less.
            if self._throttle > 0:
                if speed > self._throttle:
                    # set the delay in an attempt to throttle
                    self._delay = self._total_bytes/self._throttle - dt
                    if self._delay < 0:
                        self._delay = 0
                    self._delay /= self.threads
                    log.debug("Setting delay to: %f" % self._delay)
                else:
                    self._delay = 0

        self._bytes += bytes
        self._lock.release()

    def get_segment(self, order, remove=True, timeout=10):
        start = time.time()
        while time.time()-start < timeout:
            if self._articles.has_key(order):
                if remove:
                    return self._articles.pop(order)[0]
                return self._articles[order][0]
            time.sleep(0.1)

    def get_speed(self, pretty=False):
        #now = time.time()
        #dt = now-self._timer
        #if dt == 0:
        #    return 0
        #
        #speed = self._bytes/dt
        speed = self._total_bytes/(time.time()-self._start_time)
        if pretty:
            return sizeof_fmt(speed, True)
        return speed
        #bytes, start, stop = 0, 0, 0
        #for i in self._articles.values()[:self.threads*2]:
        #    bytes += len(i[0])
        #
        #    if start == 0:      start = i[1]
        #    if stop  == 0:      stop  = i[2]
        #    if i[1] < start:    start = i[1]
        #    if i[2] > stop:     stop  = i[2]

        #dt = stop-start
        #if dt == 0:
        #    return 0

        #speed = bytes/dt
        #if pretty:
        #    return sizeof_fmt(speed)

        #return speed

    def connect(self):
        nntp_kwargs = {
            "host":     self.host,
            "port":     self.port,
            "user":     self.user,
            "password": self.password,
            "use_ssl":  self.use_ssl,
            "timeout":  self.timeout
        }

        for c in range(self.threads):
            tid = "NNTP-%s" % (c+1)
            log.debug("Starting thread %d" % (c+1))
            t = NNTPThread(tid, self, nntp_kwargs)
            t.start()
            self._pool.append(t)

    def quit(self):
        for t in self._pool:
            t.quit()
            t.join()
        self._pool = []
