import getopt
import logging
import nntplib
import pynzb
import signal
import sys

from nzbverify import conf
from nzbstream import __version__, rar, nntp

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
    -h              : Show help text and exit
"""

DEFAULT_CONFIG_PATHS    = ['~/.nzbstream', '~/.netrc']
DEFAULT_NUM_CONNECTIONS = 1

log = logging.getLogger('nzstream')

def print_usage():
    print __usage__ % __prog__

def main(file_name, nntp_kwargs):
    nzb_file = open(file_name)
    nzb      = None
    rs       = None
    server   = None

    # Listen for exit
    def signal_handler(signal, frame):
        sys.stdout.write('\n')
        sys.stdout.write("Stopping threads...")
        sys.stdout.flush()
        if server:
            server.quit()
        sys.stdout.write("done\n")
        sys.exit(0)
    
    # TODO: Listen to other signals
    signal.signal(signal.SIGINT, signal_handler)

    
    print "Parsing NZB: %s" % file_name
    nzb = pynzb.nzb_parser.parse(nzb_file.read())

    print "Looking for Rar archives"
    rs = rar.RarSet(nzb)

    print "  Found %d Rars" % len(rs.rars)

    print "Connectiong to server"
    server = nntp.NNTP(**nntp_kwargs)

    print "Starting stream"
    num_segments = 0
    for rarfile in rs.rars:
        for segment in rarfile.segments:
            server.add_segment(segment, num_segments)
            num_segments += 1
            #f = open('cache/%s' % seg.message_id, 'rb')

    log.debug("Queued %d segments" % num_segments)
    for segment in range(num_segments):
        while True:
            data = server.get_segment(segment, timeout=2)
            if not data:
                log.debug("Segment %d not downloaded...waiting" % segment)
                continue

            ret = rs.read(data)
            if ret:
                current_bytes, total_bytes = ret
                sys.stdout.write("Progress: %3.2f%%, Speed: %s\r" % (float(current_bytes)/total_bytes*100, server.get_speed(True)))
                sys.stdout.flush()
                if current_bytes == total_bytes:
                    print "Stream completed"
                break


def run():
    print "%s version %s" % (__prog__, __version__)
        
    num_connections = DEFAULT_NUM_CONNECTIONS
    config          = None
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
    opts, args = getopt.getopt(sys.argv[1:], 's:u:P:n:c:eph', ["server=", "username=",  "port=", "connections=", "config=", "ssl", "password", "help"])
    for o, a in opts:
        if o in ("-h", "--help"):
            print __help__
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

    main(nzb, nntp_kwargs)