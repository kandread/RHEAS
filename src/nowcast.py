""" RHEAS module for nowcast simulations.

.. module:: nowcast
   :synopsis: Module that contains functionality for nowcast simulations

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import vic
import config
import ensemble
import sys
import tempfile
import shutil
from assimilation import assimilate, observationDates
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import rpath
import raster
import dbio
import logging


def runVIC(dbname, options):
    """Driver function for performing a VIC nowcast simulation"""
    if any(opt in options['vic'] for opt in ['ensemble size', 'observations']) or len(options['vic']['precip'].split(",")) > 1:
        runEnsembleVIC(dbname, options)
    else:
        runDeterministicVIC(dbname, options)


def _saveState(vicoptions):
    """Should VIC state file be saved?"""
    if 'save state' in vicoptions:
        savestate = vicoptions['save state']
        dbsavestate = True
    else:
        savestate = ""
        dbsavestate = False
    return savestate, dbsavestate


def _initialize(vicoptions):
    """Should VIC be initialized from a model state file?"""
    if 'initialize' in vicoptions:
        init = vicoptions['initialize']
    else:
        init = False
    if 'initial state' in vicoptions:
        statefile = vicoptions['initial state']
    else:
        statefile = ""
    return init, statefile


def runDeterministicVIC(dbname, options):
    """Driver function for performing a deterministic VIC nowcast simulation."""
    res = config.getResolution(options['nowcast'])
    vicexe = "{0}/vicNl".format(rpath.bins)
    basin = config.getBasinFile(options['nowcast'])
    saveto, savevars = config.getVICvariables(options)
    startyear, startmonth, startday = map(
        int, options['nowcast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['nowcast']['enddate'].split('-'))
    name = options['nowcast']['name'].lower()
    path = tempfile.mkdtemp(dir=".")
    model = vic.VIC(path, dbname, res, startyear, startmonth,
                    startday, endyear, endmonth, endday, name)
    savestate, dbsavestate = _saveState(options['vic'])
    init, statefile = _initialize(options['vic'])
    model.writeParamFile(save_state=savestate, init_state=init,
                         save_state_to_db=dbsavestate, state_file=statefile)
    model.writeSoilFile(basin)
    prec, tmax, tmin, wind = model.getForcings(options['vic'])
    model.writeForcings(prec, tmax, tmin, wind)
    model.run(vicexe)
    model.save(saveto, savevars)
    shutil.rmtree(path)


def runEnsembleVIC(dbname, options):
    res = config.getResolution(options['nowcast'])
    name = options['nowcast']['name'].lower()
    vicexe = "{0}/vicNl".format(rpath.bins)
    basin = config.getBasinFile(options['nowcast'])
    saveto, savevars = config.getVICvariables(options)
    startyear, startmonth, startday = map(
        int, options['nowcast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['nowcast']['enddate'].split('-'))
    precipdatasets = options['vic']['precip'].split(",")
    savestate, _ = _saveState(options['vic'])
    init, statefile = _initialize(options['vic'])
    if 'ensemble size' in options['vic']:
        nens = int(options['vic']['ensemble size'])
        method = "esp"
    elif 'observations' in options['vic']:
        nens = 20
        method = "esp"
    else:
        nens = len(precipdatasets)
        method = "determ"
    models = ensemble.Ensemble(nens, dbname, res, startyear,
                               startmonth, startday, endyear, endmonth, endday, name)
    if 'observations' in options['vic']:
        sdate = date(startyear, startmonth, startday) - relativedelta(months=3)
        models.setDates(sdate.year, sdate.month, sdate.day, startyear, startmonth, startday)
        models.statefiles = models.initialize("esp", basin, options)
        obsnames = options['vic']['observations'].split(",")
        updateDates = observationDates(obsnames, dbname, startyear, startmonth, startday, endyear, endmonth, endday)
        t0 = date(startyear, startmonth, startday)
        updateDates += [date(endyear, endmonth, endday)]
        for t in updateDates:
            models.setDates(t0.year, t0.month, t0.day, t.year, t.month, t.day)
            models.writeParamFiles(savestate=True)
            models.writeSoilFiles(basin)
            models.writeForcings(method, options)
            models.run(vicexe)
            models.statefiles = ["{0}/{1}".format(model.model_path, model.statefile) for model in models]
            t0 = t
            data, alat, alon, agid = assimilate(options, t, models)
            if bool(data):
                models.updateStateFiles(data, alat, alon, agid)
            models.save(saveto, savevars)
            db = dbio.connect(models.dbname)
            cur = db.cursor()
            sql = "select tablename from pg_tables where schemaname='{0}' and tablename!='dssat'".format(models.name)
            cur.execute(sql)
            for tbl in cur.fetchall():
                dbio.deleteRasters(models.dbname, "{0}.{1}".format(models.name, tbl[0]), t)
    else:
        statefile = models[0].stateFile()
        models.writeParamFiles(statefile=statefile)
        models.writeSoilFiles(basin)
        models.writeForcings(method, options)
        models.run(vicexe)
        models.save(saveto, savevars)
    for varname in savevars:
        raster.stddev(models.dbname, "{0}.{1}".format(
            models.name, varname))
        raster.mean(models.dbname, "{0}.{1}".format(
            models.name, varname))
    for e in range(nens):
        shutil.rmtree(models[e].model_path)


def runDSSAT(dbname, options):
    """Driver function for performing a DSSAT nowcast simulation"""
    log = logging.getLogger(__name__)
    startyear, startmonth, startday = map(
        int, options['nowcast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['nowcast']['enddate'].split('-'))
    res = float(options['nowcast']['resolution'])
    nens = int(options['dssat']['ensemble size'])
    name = options['nowcast']['name'].lower()
    if 'shapefile' in options['dssat']:
        shapefile = options['dssat']['shapefile']
    else:
        shapefile = None
    if 'assimilate' in options['dssat']:
        assimilate = options['dssat']['assimilate']
    else:
        assimilate = "Y"
    crops = options['dssat']['crop'].split(",")
    for crop in crops:
        crop = crop.strip()
        try:
            mod = __import__("dssat.{0}".format(crop), fromlist=[crop])
        except ImportError:
            log.error("Error in crop selected. Cannot run nowcast for {}.".format(crop))
        else:
            model = mod.Model(dbname, name, res, startyear, startmonth, startday,
                              endyear, endmonth, endday, nens, options['vic'], shapefile, assimilate)
            model.run()
            shutil.rmtree(model.path)


def execute(dbname, options):
    """Driver routine for a nowcast simulation."""
    log = logging.getLogger(__name__)
    nowcast_options = options['nowcast']
    if 'model' in nowcast_options:
        if 'vic' in nowcast_options['model']:
            if 'vic' in options:
                runVIC(dbname, options)
            else:
                log.error("No configuration options for VIC model.")
                sys.exit()
        if 'dssat' in nowcast_options['model']:
            if 'dssat' in options:
                runDSSAT(dbname, options)
            else:
                log.error("No configuration options for DSSAT model.")
                sys.exit()
    else:
        log.error("No model selected for nowcast.")
        sys.exit()
