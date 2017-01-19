
__author__ = 'mworden'

from mi.core.log import get_logger
log = get_logger()


import unittest
import os
from mi.dataset.driver.vel3d_k.wfp_stc.vel3d_k_wfp_stc_telemetered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler


class DriverTest(unittest.TestCase):

    def test_one(self):

        source_file_path = os.path.join('mi','dataset','driver','vel3d_k','wfp_stc','resource',
                                      'A0000001_WithBeams.DEC')

        particle_data_handler = ParticleDataHandler()

        particle_data_handler = parse(None, source_file_path, particle_data_handler)

        if len(particle_data_handler._samples) > 0:
            print "Found the following particles:"
            for sample in particle_data_handler._samples:
                print sample

        print "FAILURE Flag: ", particle_data_handler._failure

        self.assertEquals(particle_data_handler._failure, False)


if __name__ == '__main__':
    test = DriverTest('test_one')
    test.test_one()
