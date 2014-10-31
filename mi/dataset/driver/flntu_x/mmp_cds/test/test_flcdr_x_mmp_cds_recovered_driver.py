#!/home/mworden/uframes/ooi/uframe-1.0/python/bin/python

__author__ = 'Jeff Roy'

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config

import unittest
import os
from mi.dataset.driver.flntu_x.mmp_cds.flcdr_x_mmp_cds_recovered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler


class SampleTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join('mi', 'dataset', 'driver',
                                      'flntu_x', 'mmp_cds', 'resource',
                                      'flcdr_1_20131124T005004_459.mpk')

        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(Config().base_dir(), sourceFilePath, particle_data_hdlr_obj)

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()