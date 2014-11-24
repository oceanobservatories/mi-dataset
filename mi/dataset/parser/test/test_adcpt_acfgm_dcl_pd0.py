#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_adcpt_acfgm_dcl_pd0
@fid marine-integrations/mi/dataset/parser/test/test_adcpt_acfgm_dcl_pd0.py
@author Jeff Roy
@brief Test code for a adcpt_acfgm_dcl_pd0 data parser
"""
import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import RecoverableSampleException
from mi.idk.config import Config
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.adcpt_acfgm_dcl_pd0 import AdcptAcfgmDclPd0Parser

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver',
                             'adcpt_acfgm', 'dcl', 'pd0', 'resource')


@attr('UNIT', group='mi')
class AdcptAcfgmPd0DclParserUnitTestCase(ParserUnitTestCase):
    """
    Adcp_jln Parser unit test suite
    """
    def state_callback(self, state, fid_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.fid_ingested_value = fid_ingested

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config_recov = {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpt_acfgm_dcl_pd0',
                             DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcptAcfgmPd0DclInstrumentRecoveredParticle'}

        self.config_telem = {DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.adcpt_acfgm_dcl_pd0',
                             DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcptAcfgmPd0DclInstrumentParticle'}

        self.fid_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()

            fid.write('  - _index: %d\n' %(i+1))

            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.16f\n' % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def test_recov(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        with open(os.path.join(RESOURCE_PATH, '20140424.adcpt.log'), 'rU') as stream_handle:
            parser = AdcptAcfgmDclPd0Parser(self.config_recov,
                                            stream_handle,
                                            self.exception_callback,
                                            self.state_callback,
                                            self.publish_callback)

            particles = parser.get_records(20)
            # ask for 20 but should only get 15

            log.debug('got back %d particles', len(particles))

            # Note the yml file was produced from the parser output but was hand verified
            # against the sample outputs provided in the IDD
            self.assert_particles(particles, '20140424.recov.adcpt.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_telem(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        with open(os.path.join(RESOURCE_PATH, '20140424.adcpt.log'), 'rb') as stream_handle:
            parser = AdcptAcfgmDclPd0Parser(self.config_telem,
                                            stream_handle,
                                            self.exception_callback,
                                            self.state_callback,
                                            self.publish_callback)

            particles = parser.get_records(20)
            # ask for 20 but should only get 15

            log.debug('got back %d particles', len(particles))

            self.assert_particles(particles, '20140424.telem.adcpt.yml', RESOURCE_PATH)
            self.assertEqual(self.exception_callback_value, [])

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        #20140424.adcpt_BAD.log has a corrupt record in it
        with open(os.path.join(RESOURCE_PATH, '20140424.adcpt_BAD.log'), 'rb') as stream_handle:
            parser = AdcptAcfgmDclPd0Parser(self.config_recov,
                                            stream_handle,
                                            self.exception_callback,
                                            self.state_callback,
                                            self.publish_callback)

            #try to get a particle, should get none
            parser.get_records(1)
            log.debug('Exceptions : %s', self.exception_callback_value)
            self.assert_(isinstance(self.exception_callback_value[0], RecoverableSampleException))

    def test_live_data(self):
        files_without_records = [
            '20140424.adcpt_BAD.log',
            '20141007.adcpt.log',
            '20141008.adcpt.log',
        ]
        for filename in os.listdir(RESOURCE_PATH):
            if filename.endswith('.log'):
                log.info('Testing file: %s', filename)
                with open(os.path.join(RESOURCE_PATH, filename), 'rb') as fh:

                    parser = AdcptAcfgmDclPd0Parser(self.config_telem,
                                                    fh,
                                                    self.exception_callback,
                                                    self.state_callback,
                                                    self.publish_callback)

                    particles = parser.get_records(100)

                    log.debug('got back %d particles', len(particles))
                    if filename not in files_without_records:
                        self.assertGreater(len(particles), 0)

