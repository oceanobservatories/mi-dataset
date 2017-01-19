

from mi.core.log import get_logger


import unittest
import os
from mi.dataset.driver.wc_wm.cspp.wc_wm_cspp_recovered_driver import parse
from mi.dataset.driver.ctdpf_ckl.wfp.resource import RESOURCE_PATH
from mi.dataset.dataset_driver import ParticleDataHandler

__author__ = 'mworden'
log = get_logger()


class DriverTest(unittest.TestCase):

    def test_one(self):

        source_file_path = os.path.join(RESOURCE_PATH, 'C0000034.dat')

        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        for sample in particle_data_handler._samples:
            print sample


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()
