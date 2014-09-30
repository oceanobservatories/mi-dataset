from mi.core.log import get_logger
log = get_logger()
from mi.dataset.parser.metbk_a_dcl import MetbkADclRecoveredParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    with open(sourceFilePath, "r") as stream_handle:
        parser = MetbkADclRecoveredParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.metbk_a_dcl',
             DataSetDriverConfigKeys.PARTICLE_CLASS: 'MetbkADclRecoveredInstrumentDataParticle'},
            stream_handle,
            None,
            lambda state, ingested: None,
            lambda data: log.trace("Found data: %s", data),
            lambda ex: particleDataHdlrObj.setParticleDataCaptureFailure()
        )

        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj
