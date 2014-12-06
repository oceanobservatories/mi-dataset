# #
# OOIPLACEHOLDER
#
# #

__author__ = "ehahn"

import os
import sys

from mock import Mock
import functools
import time

from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.moas.gl.ctdgv.ctdgv_m_glider_telemetered_driver import parse as ctdgv
from mi.dataset.driver.moas.gl.dosta.dosta_abcdjm_glider_telemetered_driver import parse as dosta
from mi.dataset.driver.moas.gl.parad.parad_m_glider_telemetered_driver import parse as parad
from mi.dataset.driver.moas.gl.flort_m.flort_m_glider_telemetered_driver import parse as flort
from mi.dataset.driver.moas.gl.engineering.glider_eng_glider_telemetered_driver import parse as engineering

from mi.dataset.driver.moas.gl.ctdgv.ctdgv_m_glider_recovered_driver import parse as ctdgv_r
from mi.dataset.driver.moas.gl.dosta.dosta_abcdjm_glider_recovered_driver import parse as dosta_r
from mi.dataset.driver.moas.gl.parad.parad_m_glider_recovered_driver import parse as parad_r
from mi.dataset.driver.moas.gl.flort_m.flort_m_glider_recovered_driver import parse as flort_r
from mi.dataset.driver.moas.gl.engineering.glider_eng_glider_recovered_driver import parse as engineering_r



mock = Mock()
cwd = os.getcwd()

def profile(f, parse):
    import hotshot, hotshot.stats
    print f
    prof = hotshot.Profile("glider.prof")
    try:
        func = functools.partial(parse, cwd, f, mock)
        prof.runcall(func)
    except:
        print 'exception'
    finally:
        prof.close()

    stats = hotshot.stats.load('glider.prof')
    stats.strip_dirs()
    stats.sort_stats('time', 'calls')
    stats.print_stats(10)

def timeit():
    overall_start = time.time()
    p = ParticleDataHandler()
    for f in sys.argv[1:]:
        for parser, parse in [
            ('ctdgv', ctdgv),
            ('dosta', dosta),
            ('parad', parad),
            ('flort', flort),
            ('engineering', engineering),
            ('ctdgv_r', ctdgv_r),
            ('dosta_r', dosta_r),
            ('parad_r', parad_r),
            ('flort_r', flort_r),
            ('engineering_r', engineering_r)
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
            print '%s : %5.2f' % (f, elapsed)
            if elapsed > 3:
                profile(f, parse)

    print 'all files parsed in %6.2f seconds' % (time.time()-overall_start)
    return p

p = timeit()

for k in p._samples:
    print k, len(p._samples[k])
