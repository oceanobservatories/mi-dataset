# #
# OOIPLACEHOLDER
#
# #

import os
import sys

from mock import Mock
import functools
import time

from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.cg_dcl_eng.dcl.cg_dcl_eng_dcl_recovered_driver import parse as recov
from mi.dataset.driver.cg_dcl_eng.dcl.cg_dcl_eng_dcl_telemetered_driver import parse as telem



mock = Mock()
cwd = os.getcwd()

def profile(f, parse):
    import hotshot, hotshot.stats
    print f
    prof = hotshot.Profile("test.profile")
    try:
        func = functools.partial(parse, cwd, f, mock)
        prof.runcall(func)
    except:
        print 'exception'
    finally:
        prof.close()

    stats = hotshot.stats.load('test.profile')
    stats.strip_dirs()
    stats.sort_stats('time', 'calls')
    stats.print_stats(30)

def timeit():
    overall_start = time.time()
    p = ParticleDataHandler()
    for f in sys.argv[1:]:
        for parser, parse in [
            ('recov', recov),
            ('telem', telem),
        ]:
            print f, parser,
            try:
                start = time.time()
                parse(cwd, f, p)
            except Exception as e:
                print 'exception: %s' % e
            finally:
                stop = time.time()

            elapsed = stop-start
            print '%5.2f' % elapsed
            if elapsed > 2:
                profile(f, parse)

    print 'all files parsed in %6.2f seconds' % (time.time()-overall_start)
    return p

p = timeit()

for k in p._samples:
    print k, len(p._samples[k])
