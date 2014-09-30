from mi.dataset.parser.pco_a_2a_dcl import process, TELEMETERED_AIR_PARTICLE_CLASS


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    process(sourceFilePath, particleDataHdlrObj, TELEMETERED_AIR_PARTICLE_CLASS)

    return particleDataHdlrObj