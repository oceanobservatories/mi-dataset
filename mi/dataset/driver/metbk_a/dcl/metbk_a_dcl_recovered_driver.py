from mi.dataset.parser.metbk_a_dcl import process, RECOVERED_PARTICLE_CLASS


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    process(sourceFilePath, particleDataHdlrObj, RECOVERED_PARTICLE_CLASS)

    return particleDataHdlrObj
