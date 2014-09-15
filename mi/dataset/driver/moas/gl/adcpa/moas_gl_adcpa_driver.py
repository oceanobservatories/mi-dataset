#!/usr/local/bin/python2.7

import sys
import os

try:
    if basePythonCodePath is not None:
        pass
except NameError:
    basePythonCodePath = os.curdir

sys.path.append(basePythonCodePath)

from mi.logging import config
config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.dataset_driver import DataSetDriver, ParticleDataHandler

from mi.dataset.parser.adcp_pd0 import AdcpPd0Parser
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

config = {
    DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpa_m_glider',
    DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcpaMGliderInstrumentParticle'
}

try:
    if particleDataHdlrObj is not None:
        pass
except NameError:
    particleDataHdlrObj = ParticleDataHandler()

try:
    if sourceFilePath is not None:
        pass
except NameError:
    try:
        sourceFilePath = sys.argv[1]
    except IndexError:
        print "Need a source file path"
        sys.exit(1)


def state_callback(state, ingested):
    pass


def pub_callback(data):
    log.trace("Found data: %s", data)


def exception_callback(exception):
    particleDataHdlrObj.setParticleDataCaptureFailure()


stream_handle = open(sourceFilePath, 'rb')

try:
    parser = AdcpPd0Parser(config, None, stream_handle,
                           state_callback, pub_callback,
                           exception_callback)

    driver = DataSetDriver(parser, particleDataHdlrObj)

    driver.processFileStream()

finally:
    stream_handle.close()
