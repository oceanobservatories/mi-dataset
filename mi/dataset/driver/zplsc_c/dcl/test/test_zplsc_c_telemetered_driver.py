#!/usr/bin/env python

__author__ = 'rronquillo'

import os
import unittest

from mi.idk.config import Config
from mi.logging import log
from mi.dataset.driver.zplsc_c.dcl.zplsc_c_dcl_telemetered_driver import parse
from mi.dataset.dataset_driver import ParticleDataHandler


class DriverTest(unittest.TestCase):

    sourceFilePath = os.path.join('mi', 'dataset', 'driver', 'zplsc_c', 'dcl', 'resource',
                                  '20150407.zplsc.log')

    def test_one(self):

        particle_data_hdlr_obj = parse(Config().base_dir(), self.sourceFilePath,
                                       ParticleDataHandler())

        log.info("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.info("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)

if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()

