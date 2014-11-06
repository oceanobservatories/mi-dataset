
__author__ = 'mworden'

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config

import unittest
import os
from mi.dataset.driver.ctdmo_ghqr.sio.ctdmo_ghqr_ct_recovered_driver import parse

from mi.dataset.dataset_driver import ParticleDataHandler

class SerialNumToInductiveIdMapHandler(object):

    def __init__(self):

        self.dataDict = dict()

    def addMapping(self, serialNum, inductiveId):

        self.dataDict[serialNum] = inductiveId

    def getInductiveId(self, serialNum):

        return self.dataDict.get(serialNum)


class SampleTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_one(self):

        sourceFilePath = os.path.join('mi','dataset','driver','ctdmo_ghqr','sio',
                                      'resource','SBE37-IM_20141231_2014_12_31.hex')
        particle_data_hdlr_obj = ParticleDataHandler()

        serial_num_to_inductive_id_map_handler = SerialNumToInductiveIdMapHandler()

        serial_num_to_inductive_id_map_handler.addMapping(20141231, 55)

        particle_data_hdlr_obj = parse(Config().base_dir(),
                                       sourceFilePath,
                                       particle_data_hdlr_obj,
                                       serial_num_to_inductive_id_map_handler)

        print particle_data_hdlr_obj._samples
        print particle_data_hdlr_obj._failure
        log.debug("SAMPLES: %s", particle_data_hdlr_obj._samples)
        log.debug("FAILURE: %s", particle_data_hdlr_obj._failure)

        self.assertEquals(particle_data_hdlr_obj._failure, False)


if __name__ == '__main__':
    test = SampleTest('test_one')
    test.test_one()
