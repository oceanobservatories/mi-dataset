#!/home/mworden/uframes/ooi/uframe-1.0/python/bin/python

from mi.core.log import get_logger
from mi.idk.config import Config

import unittest
import os
from mi.dataset.driver.flord_l_wfp.sio.flord_l_wfp_sio_telemetered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'mworden'

log = get_logger()


class DriverTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        source_file_path = os.path.join('mi', 'dataset', 'driver', 'flord_l_wfp', 'sio', 'resource',
                                        'node58p1_0.we_wfp.dat')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(Config().base_dir(), source_file_path, particle_data_hdlr_obj)

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)

    def test_two(self):

        source_file_path = os.path.join('mi', 'dataset', 'driver', 'flord_l_wfp', 'sio', 'resource',
                                        'node17p1_33.we_wfp-new-header.dat')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(Config().base_dir(), source_file_path, particle_data_hdlr_obj)

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()
    test = DriverTest('test_two')
    test.test_two()
