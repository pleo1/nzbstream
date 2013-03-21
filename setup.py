import setuptools
from distutils.core import setup

import nzbstream

setup(
    name='nzbstream',
    version=nzbstream.__version__,
    packages=['nzbstream'],
    url='http://pypi.python.org/pypi/nzbstream/',
    license='LICENSE',
    description='Utility for streaming the contents of an NZB.',
    long_description=open('README').read(),
    scripts=['bin/nzbstream']
)