#!/usr/bin/env python

"""
@package mi.dataset.driver.fuelcell_eng.dcl.test
@file mi-dataset/mi/dataset/driver/fuelcell_eng/dcl/test_fuelcell_eng_dcl_telemetered_driver.py
@author Chris Goodrich
@brief Sample test for test_fuelcell_eng_dcl_telemetered_driver

Release notes:

Initial Release
"""

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config

import unittest
import os
from mi.dataset.driver.fuelcell_eng.dcl.fuelcell_eng_dcl_telemetered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler


class SampleTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join('mi', 'dataset', 'driver',
                                      'fuelcell_eng', 'dcl', 'resource',
                                      '20141207s.pwrsys.log')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(Config().base_dir(), sourceFilePath, particle_data_hdlr_obj)

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()
