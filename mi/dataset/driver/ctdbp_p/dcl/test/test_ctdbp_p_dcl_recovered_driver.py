_author__ = 'jeff roy'

from mi.core.log import get_logger
log = get_logger()


import unittest
import os
from mi.dataset.driver.ctdbp_p.dcl.ctdbp_p_dcl_recovered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler


class DriverTest(unittest.TestCase):

    def test_one(self):

        source_file_path = os.path.join('mi',
                                      'dataset', 'driver', 'ctdbp_p',
                                      'dcl', 'resource',
                                      'ctdbp01_20150804_061734.DAT')

        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        log.debug("SAMPLES: %s", particle_data_handler._samples)
        log.debug("FAILURE: %s", particle_data_handler._failure)

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()
