
__author__ = 'mworden'

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config

import unittest
import os
from mi.dataset.driver.ctdmo_ghqr.sio.ctdmo_ghqr_ct_recovered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler

class SampleTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join('mi','dataset','driver','ctdmo_ghqr','sio',
                                      'resource','SBE37-IM_20141231_2014_12_31.hex')
        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(Config().base_dir(),
                                       sourceFilePath,
                                       particle_data_hdlr_obj)

        print particle_data_hdlr_obj._samples
        print particle_data_hdlr_obj._failure
        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()
