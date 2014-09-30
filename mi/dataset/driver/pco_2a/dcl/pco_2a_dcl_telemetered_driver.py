from mi.dataset.parser.pco_a_2a_dcl import Pco2aDclAirParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


def state_callback(state, file):
    pass


def pub_callback(state):
    pass


def exception_callback(state):
    pass


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    with open(sourceFilePath, "r") as fil:
        parser = Pco2aDclAirParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: "mi.dataset.parser.pco_a_2a_dcl",
             DataSetDriverConfigKeys.PARTICLE_CLASS: None},
            fil,
            None,
            state_callback,
            pub_callback,
            exception_callback);
        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()
    return particleDataHdlrObj
