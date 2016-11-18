#!/home/mworden/uframes/ooi/uframe-1.0/python/bin/python

__author__ = 'mworden'

from mi.core.log import get_logger
log = get_logger()

from mi import MI_BASE_PATH

import unittest
import os
from mi.dataset.driver.ctdmo_ghqr.imodem.ctdmo_ghqr_imodem_telemetered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler


class DriverTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join('mi','dataset','driver','ctdmo_ghqr','imodem','resource',
                                      'ctdmo01_20140712_120719.DAT')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(MI_BASE_PATH, sourceFilePath, particle_data_hdlr_obj)

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()
