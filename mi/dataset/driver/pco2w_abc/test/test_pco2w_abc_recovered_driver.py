#!/home/mworden/uframes/ooi/uframe-1.0/python/bin/python

__author__ = 'mworden'


import unittest
import os
from mi.dataset.driver.pco2w_abc.pco2w_abc_recovered_driver import parse
from mi.logging import config
from mi.core.log import get_logger

base_python_code_path = ''

config.add_configuration(os.path.join(base_python_code_path, 'res', 'config', 'mi-logging.yml'))
log = get_logger()

from mi.dataset.dataset_driver import ParticleDataHandler


class SampleTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join('mi','dataset','driver','pco2w_abc','resource','SAMI_C0069_300614.txt')
        particle_data_hdlr_obj = ParticleDataHandler()

        particle_data_hdlr_obj = parse(base_python_code_path, sourceFilePath, particle_data_hdlr_obj)

        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()