#!/usr/bin/env python

"""
@package mi.dataset.driver.ctdbp_cdef.cp
@file marine-integrations/mi/dataset/driver/ctdbp_cdef/cp/ctdbp_cdef_cp.py
@author Tapana Gupta
@brief Driver for the ctdbp_cdef_cp instrument

Release notes:

Initial Release
"""

from mi.dataset.parser.ctdbp_cdef_cp import CtdbpCdefCpParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    with open(sourceFilePath, "r") as stream_handle:
        parser = CtdbpCdefCpParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: "mi.dataset.parser.ctdbp_cdef_cp",
             DataSetDriverConfigKeys.PARTICLE_CLASS: None},
             stream_handle,
             lambda state, ingested: None,
             lambda data: None,
             lambda ex: particleDataHdlrObj.setParticleDataCaptureFailure()
        )
        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()
    return particleDataHdlrObj
