from mi.dataset.parser.metbk_a_dcl import process, TELEMETERED_PARTICLE_CLASS


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    process(sourceFilePath, particleDataHdlrObj, TELEMETERED_PARTICLE_CLASS)

    return particleDataHdlrObj
