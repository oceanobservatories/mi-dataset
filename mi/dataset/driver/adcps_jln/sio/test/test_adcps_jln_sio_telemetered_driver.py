#!/usr/bin/env python
import os
import unittest

from mi import MI_BASE_PATH
from mi.core.log import get_logger
from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.adcps_jln.sio.adcps_jln_sio_telemetered_driver import parse
from mi.dataset.driver.adcps_jln.sio.resource import RESOURCE_PATH

__author__ = 'Joe Padula'

log = get_logger()


class SampleTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join(RESOURCE_PATH, 'node59p1_2.adcps.dat')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(MI_BASE_PATH, sourceFilePath, particle_data_hdlr_obj)

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()