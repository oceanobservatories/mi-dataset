#!/usr/bin/env python

__author__ = 'rronquillo'

import os
import unittest

from mi.idk.config import Config
from mi.logging import log
from mi.dataset.driver.camhd_a.camhd_a_telemetered_driver import parse
from mi.dataset.dataset_driver import ParticleDataHandler


class DriverTest(unittest.TestCase):

    sourceFilePath = os.path.join(
        'mi', 'dataset', 'driver', 'camhd_a', 'resource', 'RS03ASHS-PN03B-06-CAMHDA301', '2015',
        '11', '19', 'CAMHDA301-20151119T210000Z.mp4')

    def test_one(self):

        particle_data_hdlr_obj = parse(Config().base_dir(), self.sourceFilePath,
                                       ParticleDataHandler())

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)

if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()

