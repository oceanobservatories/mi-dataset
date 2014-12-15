
"""
@package mi.dataset.driver.spkir_abj.cspp.test
@file mi/dataset/driver/spikr_abj/cspp/test/test_spkir_abj_cspp_recovered_driver.py
@author Mark Worden
@brief Minimal test code to exercise the driver parse method for spkir_abj_cspp recovered

Release notes:

Initial Release
"""

__author__ = 'mworden'

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config

import unittest
import os
from mi.dataset.driver.spkir_abj.cspp.spkir_abj_cspp_recovered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler


class DriverTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join('mi', 'dataset', 'driver', 'spkir_abj', 'cspp', 'resource',
                                      '11079419_PPB_OCR.txt')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(Config().base_dir(), sourceFilePath, particle_data_hdlr_obj)

        for sample in particle_data_hdlr_obj._samples:
            print sample

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()