import getopt
import logging
import nntplib
import pynzb
import signal
import sys

from nzbverify import conf
from nzbstream import __version__, rar, nntp, manager

__prog__ = "nzbstream"

__usage__ = """
Usage:
    %s [options] <NZB file>

Options:
    -s<server>      : NNTP server
    -u<username>    : NNTP username
    -p              : NNTP password, will be prompted for
    -P<port>        : NNTP port
    -c<config>      : Config file to use (defaults: ~/.nzbstream, ~/.netrc)
    -n<threads>     : Number of NNTP connections to use
    -e              : Use SSL/TLS encryption
    -q              : Skip verification stage
    -b<bitrate>     : Maximum bitrate of file (in Bps)
    -h              : Show help text and exit
"""

DEFAULT_CONFIG_PATHS    = ['~/.nzbstream', '~/.netrc']
DEFAULT_NUM_CONNECTIONS = 1

log = logging.getLogger('nzstream')

def print_usage():
    print __usage__ % __prog__

def main(file_name, nntp_kwargs, max_bitrate=None, do_verify=True):
    nzb_file    = None
    nzb         = None
    rs          = None
    server      = None
    media_file  = None
    bitrate     = None

    # Listen for exit
    def signal_handler(signal, frame):
        sys.stdout.write("\nStopping threads...")
        sys.stdout.flush()
        if server:
            server.quit()
        sys.stdout.write("done\n")
        sys.exit(0)
    
    # TODO: Listen to other signals
    signal.signal(signal.SIGINT, signal_handler)

    mgr = manager.Manager(file_name, nntp_kwargs, max_bitrate, do_verify)

    if not mgr.initialize():
        print "[Error] Manager failed to initialize"
        return

    if not mgr.verify():
        print "[Error] Verification failed"
        return

    mgr.stream()

    return

    print "Parsing NZB: %s" % file_name
    nzb = pynzb.nzb_parser.parse(nzb_file.read())

    print "Looking for rar archives"
    rs = rar.RarSet(nzb)

    print "  Found %d rars" % len(rs.rars)

    if do_verify:
        print "Verifying all rar segments are available"
        # TODO: Verify

    print "Connecting to server"
    server = nntp.NNTP(**nntp_kwargs)

    print "Starting stream"
    num_segments = 0
    for rarfile in rs.rars:
        for segment in rarfile.segments:
            server.add_segment(segment, num_segments)
            num_segments += 1

    log.debug("Queued %d segments" % num_segments)
    for segment in range(num_segments):
        while True:
            data = server.get_segment(segment, timeout=2)
            if not data:
                log.debug("Segment %d not downloaded...waiting" % segment)
                continue

            ret = rs.read(data)
            if ret:
                if media_file is None and rs._file_name is not None:
                    media_file = rs._file_name
                    print "Found media file: ", media_file
                
                if bitrate is None:
                    bitrate = rs._fd.get_bitrate()
                    if bitrate:
                        print "  Bitrate: %s" % nntp.sizeof_fmt(bitrate)
                        if max_bitrate is not None and max_bitrate > bitrate:
                            print "  Bitrate exceeds user-defined max (%s)"
                            return

                current_bytes, total_bytes = ret

                # TODO: Write data to enable resuming

                sys.stdout.write("\rProgress: %3.2f%%, Speed: %10s" % (float(current_bytes)/total_bytes*100, server.get_speed(True)))
                sys.stdout.flush()
                if current_bytes == total_bytes:
                    print "Stream completed"
                break


def run():
    print "%s version %s" % (__prog__, __version__)
        
    num_connections = DEFAULT_NUM_CONNECTIONS
    config          = None
    max_bitrate     = None
    do_verify       = True
    nntp_kwargs     = {
        'host':     None,
        'port':     nntplib.NNTP_PORT,
        'user':     None,
        'password': None,
        'use_ssl':  None,
        'timeout':  10,
        'threads':  DEFAULT_NUM_CONNECTIONS
    }
    
    # Parse command line options
    opts, args = getopt.getopt(sys.argv[1:], 's:u:P:n:c:b:qeph', [
        "server=",
        "username=", 
        "port=",
        "connections=",
        "config=",
        "ssl",
        "password",
        "verify",
        "bitrate=",
        "help"])
    for o, a in opts:
        if o in ("-h", "--help"):
            print_usage()
            sys.exit(0)
        elif o in ("-s", "--server"):
            nntp_kwargs['host'] = a
        elif o in ("-u", "--username"):
            nntp_kwargs['user'] = a
        elif o in ("-p", "--password"):
            nntp_kwargs['password'] = getpass.getpass("Password: ")
        elif o in ("-e", "--ssl"):
            nntp_kwargs['use_ssl'] = True
        elif o in ("-P", "--port"):
            try:
                nntp_kwargs['port'] = int(a)
            except:
                print "Error: invalid port '%s'" % a
                sys.exit(0)
        elif o in ("-n", "--connections"):
            try:
                nntp_kwargs['threads'] = int(a)
            except:
                print "Error: invalid number of connections '%s'" % a
                sys.exit(0)
        elif o in ("-c", "--config"):
            config = a
        elif o in ("-b", "--bitrate"):
            try:
                max_bitrate = float(a)
            except:
                print "Error: invalid bitrate: '%s'" % a
                sys.exit(0)
        elif o in ("-q", "--verify"):
            do_verify = False
    
    # Get the NZB
    if len(args) < 1:
        print_usage()
        sys.exit(0)
    nzb = args[0]
    
    # See if we need to load certain NNTP details from config files
    # A host is required
    config = conf.get_config(config, defaults=DEFAULT_CONFIG_PATHS)
    if not nntp_kwargs['host'] and not config:
        print "Error: no server details provided"
        sys.exit(0)
    
    if config:
        credentials = config.authenticators(nntp_kwargs.get('host'))
        if not credentials:
            if not config.hosts:
                print "Error: Could not determine server details"
                sys.exit(0)
            
            # Just use the first entry
            host, credentials = config.hosts.items()[0]
            nntp_kwargs['host'] = host
        
        if not nntp_kwargs['user'] and not nntp_kwargs['password']:
            nntp_kwargs['user'] = credentials[0]
            nntp_kwargs['password'] = credentials[2]

    main(nzb, nntp_kwargs, max_bitrate, do_verify)
