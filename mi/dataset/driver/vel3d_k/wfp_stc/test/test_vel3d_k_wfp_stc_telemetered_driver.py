
__author__ = 'mworden'

from mi.core.log import get_logger
log = get_logger()

from mi import MI_BASE_PATH

import unittest
import os
from mi.dataset.driver.vel3d_k.wfp_stc.vel3d_k_wfp_stc_telemetered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler


class DriverTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join('mi','dataset','driver','vel3d_k','wfp_stc','resource',
                                      'A0000001_WithBeams.DEC')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(MI_BASE_PATH, sourceFilePath, particle_data_hdlr_obj)

        if len(particle_data_hdlr_obj._samples) > 0:
            print "Found the following particles:"
            for sample in particle_data_hdlr_obj._samples:
                print sample

        print "FAILURE Flag: ", particle_data_hdlr_obj._failure

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()
