
__author__ = 'jroy'

from mi.core.log import get_logger
log = get_logger()


import unittest
import os
from mi.dataset.driver.adcpa_n.adcpa_n_recovered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler
from mi.dataset.driver.adcpa_n.resource import RESOURCE_PATH
from mi import MI_BASE_PATH


class DriverTest(unittest.TestCase):
    def test_one(self):

        sourceFilePath = os.path.join(RESOURCE_PATH, 'adcp_auv_51.pd0')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(MI_BASE_PATH, sourceFilePath, particle_data_hdlr_obj)

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()