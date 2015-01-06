
__author__ = 'mworden'

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config

import unittest
import os
from mi.dataset.driver.wc_wm.cspp.wc_wm_cspp_telemetered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler


class DriverTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join('mi', 'dataset', 'driver', 'ctdpf_ckl', 'wfp', 'resource',
                                      'C0000034.dat')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(Config().base_dir(), sourceFilePath, particle_data_hdlr_obj)

        for sample in particle_data_hdlr_obj._samples:
            print sample


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()