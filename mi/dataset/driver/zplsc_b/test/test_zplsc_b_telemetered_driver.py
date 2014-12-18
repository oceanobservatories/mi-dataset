#!/usr/bin/env python

__author__ = 'rronquillo'

import os
import unittest

from mi.idk.config import Config
from mi.logging import log
from mi.dataset.driver.zplsc_b.zplsc_b_telemetered_driver import parse
from mi.dataset.dataset_driver import ParticleDataHandler


class DriverTest(unittest.TestCase):

    # The file below contains bathtub data.
    sourceFilePath = os.path.join('mi', 'dataset', 'driver', 'zplsc_b', 'resource',
                                  'OOI-D20141212-T152500.raw')

    # The file below contains data collected at sea, but has a file size of 104.9 MB,
    # this exceeds GitHub's file size limit of 100 MB.
    # sourceFilePath = os.path.join('mi', 'dataset', 'driver', 'zplsc_b', 'resource',
    #                               'Baja 2013 - -D20131020-T030020.raw')

    # For testing, just output to the same test directory.
    # The real output file path will be set in the spring xml file.
    outputFilePath = os.path.join('mi', 'dataset', 'driver', 'zplsc_b', 'test')

    def test_one(self):

        particle_data_hdlr_obj = parse(Config().base_dir(), self.sourceFilePath,
                                       self.outputFilePath, ParticleDataHandler())

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)

if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()

