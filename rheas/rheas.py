""" RHEAS module for main functionality.

.. module:: rheas
   :synopsis: Module for executing the RHEAS system

.. moduleauthor:: Kostas Andreadis <kandread@umass.edu>

"""

import argparse
import logging
from datetime import datetime
from importlib import import_module

from . import config, datasets, dbio, forecast, nowcast


def parseArgs():
    """Parses command line arguments and prints help messages."""
    parser = argparse.ArgumentParser(description='Runs RHEAS simulation.')
    parser.add_argument('config', help='configuration file')
    parser.add_argument('-d', metavar='DB', help='name of database to connect')
    parser.add_argument('-u', '--update', help='update database', action='store_true')
    parser.add_argument('-v', '--verbose', help='increase verbosity', action='store_true')
    parser.add_argument('-l', metavar='logfile', help='name of log file')
    args = parser.parse_args()
    return args.config, args.d, args.update, args.verbose, args.l


def update(dbname, configfile):
    """Fetch datasets and update database."""
    log = logging.getLogger(__name__)
    conf = datasets.readDatasetList(configfile)
    try:
        bbox = [conf.getfloat('domain', s) for s in ['minlon', 'minlat', 'maxlon', 'maxlat']]
    except:
        bbox = None
    for name in conf.sections():
        if name != 'domain':
            try:
                mod = import_module("rheas.datasets.{0}".format(name))
            except ModuleNotFoundError:
                mod = None
            if conf.has_option(name, 'startdate'):
                t0 = datetime.strptime(conf.get(name, 'startdate'), "%Y-%m-%d")
            else:
                t0 = None
            if conf.has_option(name, 'enddate'):
                t1 = datetime.strptime(conf.get(name, 'enddate'), "%Y-%m-%d")
            else:
                t1 = datetime.today()
            if mod is None:
                # download generic datasets
                datasets.download(dbname, (t0, t1), bbox, conf, name)
            else:
                dt = mod.dates(dbname)
                if t0 is None:
                    if dt is None:
                        log.warning("Date information for {0} not found in the database or data.conf. Please add a startdate in the data.conf file.".format(name))
                    else:
                        dt = (dt[0], t1)
                else:
                    dt = (t0, t1)
                if dt is not None:
                    mod.download(dbname, dt, bbox)
                    if isinstance(mod.table, list):
                        invalid = []
                        for table in mod.table:
                            invalid.append(datasets.validate(dbname, table, dt))
                        invalid = list(set(invalid))
                    else:
                        invalid = datasets.validate(dbname, mod.table, dt)
                    for t in invalid:
                        mod.download(dbname, (t, t), bbox)


def run():
    """Main RHEAS routine."""
    config_filename, dbname, db_update, verbose, logfile = parseArgs()
    if verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    if logfile is None:
        logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(filename=logfile, level=log_level, format='%(levelname)s: %(message)s')
    log = logging.getLogger(__name__)
    if dbname is None:
        dbname = "rheas"
    dbio.connect(dbname)
    # check if database update is requested
    if db_update:
        log.info("Updating database!")
        update(dbname, config_filename)
    else:
        options = config.loadFromFile(config_filename)
        # check what simulations have been requested
        if "nowcast" in options:
            nowcast.execute(dbname, options)
        if "forecast" in options:
            forecast.execute(dbname, options)
