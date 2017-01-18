#!/usr/bin/env python

"""
@package mi.dataset.driver.pco2a_a.dcl.pco2a_a_dcl_telemetered_driver
@file mi/dataset/driver/pco2a_a/dcl/pco2a_a_dcl_telemetered_driver.py
@author Sung Ahn
@brief Telemetered driver for pco2a_a_dcl data parser.

"""

from mi.dataset.driver.pco2a_a.dcl.pco2a_a_dcl_driver import process, \
    TELEMETERED_PARTICLE_CLASSES
from mi.core.versioning import version


@version("15.6.2")
def parse(unused, sourceFilePath, particleDataHdlrObj):
    process(sourceFilePath, particleDataHdlrObj, TELEMETERED_PARTICLE_CLASSES)

    return particleDataHdlrObj
