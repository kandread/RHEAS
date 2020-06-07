""" RHEAS configuration file parser module.

.. module:: config
   :synopsis: Module for parsing RHEAS configurations files

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import logging
import os
import re
import sys
from configparser import ConfigParser
from io import StringIO


def _readFromFile(config_filename):
    """Reads a RHEAS configuration from a file."""
    log = logging.getLogger(__name__)
    conf = ConfigParser()
    try:
        conf.read(config_filename)
    except:
        log.error("File not found: {}".format(config_filename))
        sys.exit()
    return conf


def _parseConfig(config):
    """Parses configuration object into dictionary of options."""
    options = {}
    for section in config.sections():
        options[section] = {}
        for item in config.items(section):
            options[section.lower()][item[0]] = item[1]
    return options


def _checkOptions(options):
    """Checks for minimum required options in the RHEAS configuration."""
    log = logging.getLogger(__name__)
    if 'nowcast' in options:
        simtype = 'nowcast'
    elif 'forecast' in options:
        simtype = 'forecast'
    else:
        log.error("No configuration found for either a nowcast or a forecast. Exiting...")
        sys.exit()
    if not all((opt in options[simtype] for opt in ('model', 'startdate', 'enddate', 'name', 'basin', 'resolution'))):
        log.error("Missing options for {0}. Need (model, startdate, enddate, name, basin, resolution) options. Exiting...".format(
            simtype))
        sys.exit()
    if 'resolution' in options[simtype]:
        res = options[simtype]['resolution']
        if res < 0:
            log.error("Bad value for spatial resolution ({0}). Exiting...".format(res))
            sys.exit()


def loadFromFile(config_filename):
    """Loads a RHEAS configuration from a file."""
    conf = _readFromFile(config_filename)
    options = _parseConfig(conf)
    for section in options:
        if 'initialize' in options[section]:
            try:
                options[section]['initialize'] = conf.getboolean(
                    section, 'initialize')
            except:
                pass
        if 'assimilate' in options[section]:
            try:
                options[section]['assimilate'] = conf.getboolean(
                    section, 'assimilate')
            except:
                options[section]['assimilate'] = conf.get(section, 'assimilate')
    _checkOptions(options)
    return options


def getResolution(options):
    """Get spatial resolution from configuration options."""
    log = logging.getLogger(__name__)
    try:
        res = float(options['resolution'])
    except:
        try:
            s = re.search("([0-9]+)([a-z]+)", options['resolution'].lower())
            units = {'k': 1.0, 'm': 1000.0}
            res = float(s.group(1)) / (110.0 * units[s.group(2)[0]])
        except:
            log.error("No appropriate resolution has been set. Exiting!")
            sys.exit()
    return res


def getVICExecutable(options):
    """Get VIC executable from configuration options or set it to
    default location of not given."""
    if 'exe' in options:
        vicexe = options['exe']
    else:
        vicexe = os.path.dirname(os.path.abspath(__file__)) + "/../external/vic/src/vicNl"
    return vicexe


def getBasinFile(options):
    """Get basin file name from configuration options."""
    log = logging.getLogger(__name__)
    basin = None
    if 'basin' in options and os.path.isfile(options['basin']):
        basin = options['basin']
    else:
        log.error("Basin file {0} not provided or does not exist. Exiting!".format(basin))
        sys.exit()
    return basin


def getVICvariables(options):
    """Get list of VIC variables and format to save."""
    if 'save to' in options['vic']:
        saveto = options['vic']['save to']
        if 'save' in options['vic']:
            savevars = [s.strip() for s in options['vic']['save'].split(',')]
        else:
            savevars = []
        simtype = [k for k in options if k == 'nowcast' or k == 'forecast'][0]
        models = [s.strip() for s in options[simtype]['model'].split(",")]
        if 'dssat' in models:
            for v in ['rainf', 'net_short', 'net_long', 'soil_moist', 'tmax', 'tmin']:
                if v not in savevars:
                    savevars.insert(0, v)
            # if 'lai' in options['vic'] and not 'lai' in savevars:
            #     savevars.append('lai')
        if any(v in savevars for v in ['drought', 'cdi', 'severity', 'smdi']) and 'soil_moist' not in savevars:
            savevars.insert(0, 'soil_moist')
        if any(v in savevars for v in ['drought', 'cdi']) and 'par' not in savevars:
            savevars.insert(0, 'par')
        if any(v in savevars for v in ['drought', 'spi', 'cdi']) and 'rainf' not in savevars:
            savevars.insert(0, 'rainf')
        if any(v in savevars for v in ['drought', 'severity', 'sri']) and 'runoff' not in savevars:
            savevars.insert(0, 'runoff')
        for dvar in ['sri', 'spi']:
            if dvar in savevars:
                savevars.remove(dvar)
                savevars.extend(["{0}{1}".format(dvar, dur) for dur in (1, 3, 6, 12)])
        if 'observations' in options['vic']:
            obsnames = options['vic']['observations'].split(",")
            # FIXME: make this more dynamic by having the observation module
            # use its state variable and observation attributes
            if any(n in obsnames for n in ['amsre', 'smos', 'smap', 'windsat']) and 'soil_moist' not in savevars:
                savevars.append('soil_moist')
            if any(n in obsnames for n in ['mod16', 'ptjpl']):
                if 'evap' not in savevars:
                    savevars.append('evap')
                if 'soil_moist' not in savevars:
                    savevars.append('soil_moist')
    else:
        saveto = None
    return saveto, savevars
