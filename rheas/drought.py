""" RHEAS module for generating drought products.

.. module:: drought
   :synopsis: Module that contains functionality for generating drought products

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import logging
from datetime import date, datetime

import numpy as np
import pandas as pd
import scipy.stats as stats
from dateutil.relativedelta import relativedelta

from . import dbio


def _clipToValidRange(data):
    """Clip data series to valid intervals for drought index values."""
    valid_min = -3.09
    valid_max = 3.09
    return np.clip(data, valid_min, valid_max)


def _movingAverage(data, n):
    """Calculate the moving average from a time series."""
    out = np.cumsum(data)
    out[n:] = out[n:] - out[:-n]
    return out[n - 1:] / n


def calcVCI(model, table="ndvi.modis"):
    """Calculate Vegetation Condition Index."""
    log = logging.getLogger(__name__)
    sdate = date(model.startyear, model.startmonth, model.startday).strftime("%Y-%m-%d")
    edate = date(model.endyear, model.endmonth, model.endday).strftime("%Y-%m-%d")
    if dbio.tableExists(model.dbname, table.split(".")[0], table.split(".")[1]):
        db = dbio.connect(model.dbname)
        cur = db.cursor()
        cur.execute("drop table if exists ndvi_max, ndvi_min, ndvi_max_min, f1")
        db.commit()
        sql = "create table ndvi_max as (select st_union(rast, 'MAX') as rast from {0})".format(table)
        cur.execute(sql)
        sql = "create table ndvi_min as (select st_union(rast, 'MIN') as rast from {0})".format(table)
        cur.execute(sql)
        sql = "create table ndvi_max_min as (select st_mapalgebra(max.rast, 1, min.rast, 1, '[rast1]-[rast2]') as rast from ndvi_max as max, ndvi_min as min)"
        cur.execute(sql)
        db.commit()
        sql = "create table f1 as (select fdate, st_mapalgebra(f.rast, 1, min.rast, 1, '[rast1]-[rast2]') as rast from {0} as f, ndvi_min as min where fdate>=date'{1}' and fdate<=date'{2}' group by fdate,f.rast,min.rast)".format(table, sdate, edate)
        cur.execute(sql)
        db.commit()
        if dbio.tableExists(model.dbname, model.name, "vci"):
            sql = "insert into {0}.vci (fdate, rast) select fdate, st_mapalgebra(f1.rast, 1, mm.rast, 1, '[rast1]/([rast2]+0.0001)') as rast from f1, ndvi_max_min as mm group by fdate,f1.rast,mm.rast".format(model.name)
        else:
            sql = "create table {0}.vci as (select fdate, st_mapalgebra(f1.rast, 1, mm.rast, 1, '[rast1]/([rast2]+0.0001)') as rast from f1, ndvi_max_min as mm group by fdate,f1.rast,mm.rast)".format(model.name)
        cur.execute(sql)
        db.commit()
        cur.execute("drop table ndvi_max")
        cur.execute("drop table ndvi_min")
        cur.execute("drop table ndvi_max_min")
        cur.execute("drop table f1")
        db.commit()
        cur.close()
        db.close()
    else:
        log.warning("No NDVI data were found in database. Cannot calculate VCI!")
    return None


def _calcSuctionHead(model, ensemble, nlayers=3):
    """Calculate soil suction from soil moisture using the Clapp
    and Hornberger (1978) model and parameters."""
    if bool(ensemble):
        equery = "and (ensemble={0} or ensemble=0)".format(ensemble)
    else:
        equery = ""
    Ksat = np.array([63.36, 56.16, 12.49, 2.59, 2.5, 2.27, 0.612, 0.882, 0.781, 0.371, 0.461])
    Ksat *= (10 * 24.)  # convert from cm/hr to mm/day
    n = [.395, .41, .435, .485, .451, .42, .477, .476, .426, .492, .482]
    psi_a = [121., 90., 218., 786., 478., 299., 356., 630., 153., 490., 405.]
    b = [4.05, 4.38, 4.9, 5.3, 5.39, 7.12, 7.75, 8.52, 10.4, 10.4, 11.4]
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    # get georeference information
    cur.execute("select st_upperleftx(rast), st_upperlefty(rast), st_scalex(rast), st_scaley(rast) from {0}.soil_moist".format(model.name))
    results = cur.fetchone()
    ulx, uly, xres, yres = results
    # get soil moisture for surface and root zone layer
    sql = "select fdate,(ST_DumpValues(st_union(rast,'sum'))).valarray from {0}.soil_moist where layer<3 {1} group by fdate order by fdate".format(model.name, equery)
    cur.execute(sql)
    if bool(cur.rowcount):
        st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
        et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
        results = cur.fetchall()
        data = np.array([np.array(r[1]).ravel() for r in results])
        i = np.where(np.not_equal(data[0, :], None))[0]
        sm = pd.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
        pfz = np.zeros(sm[st:et].shape)
        ii, jj = np.unravel_index(i, np.array(results[0][1]).shape)
        for j in sm.columns:
            # identify soil type by saturated conductivity
            cur.execute("select line from vic.soils order by geom <-> st_geomfromtext('POINT({0} {1})', 4326) limit 1".format(ulx+xres*jj[j], uly+yres*ii[j]))
            line = cur.fetchone()[0]
            k = np.mean(np.array(map(float, line.split()[9+nlayers:nlayers+11])))
            z = sum(map(float, line.split()[4*nlayers+10:4*nlayers+12])) * 1000.
            ki = np.argmin(abs(Ksat - k))
            # convert into dekad averages
            d = sm[j].index.day - np.clip((sm[j].index.day-1) // 10, 0, 2)*10 - 1
            date = sm[j].index.values - np.array(d, dtype='timedelta64[D]')
            sm_dekad = sm[j].groupby(date).apply(np.mean)
            # calculate soil suction
            pf = np.log(psi_a[ki] * ((sm_dekad / z) / n[ki])**(-b[ki]))
            # calculate z-score of soil suction
            pf = (pf[st:et] - pf.mean()) / pf.std()
            pfz[:, j] = pf.reindex(sm[st:et].index).ffill().values
    else:
        pfz = None
    cur.close()
    db.close()
    return pfz


def _calcFpar(model, ensemble):
    """Retrieve the Photosynthetically Active Radiation from the model simulation."""
    if bool(ensemble):
        equery = "where (ensemble={0} or ensemble=0)".format(ensemble)
    else:
        equery = ""
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.par {1} order by fdate".format(model.name, equery)
    cur.execute(sql)
    if bool(cur.rowcount):
        results = cur.fetchall()
        data = np.array([np.array(r[1]).ravel() for r in results])
        i = np.where(np.not_equal(data[0, :], None))[0]
        fpar = pd.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
        d = fpar.index.day - np.clip((fpar.index.day-1) // 10, 0, 2)*10 - 1
        date = fpar.index.values - np.array(d, dtype='timedelta64[D]')
        fpar_dekad = fpar.groupby(date, axis=0).apply(np.mean)
        st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
        et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
        fparz = (fpar_dekad[st:et] - fpar_dekad.mean(axis=0)) / fpar_dekad.std(axis=0)
        fparz = fparz.reindex(fpar[st:et].index).ffill().values
    else:
        fparz = None
    cur.close()
    db.close()
    return fparz


def calcCDI(model, ensemble):
    """Calculate Combined Drought Index as a monthly time series. The index is
    categorical with the values corresponding to:
    0 = No drought
    1 = Watch (Precipitation deficit)
    2 = Warning (Soil moisture deficit)
    3 = Alert 1 (Vegetation stress following precipitation deficit)
    4 = Alert 2 (Vegetation stress following precipitation/soil moisture deficit)."""
    log = logging.getLogger(__name__)
    spi = calcSPI(3, model, ensemble)
    sma = _calcSuctionHead(model, ensemble)
    fapar = _calcFpar(model, ensemble)
    if all(v is not None for v in [spi, sma, fapar]):
        cdi = np.zeros(spi.shape, dtype='int')
        cdi[spi < -1] = 1
        cdi[(fapar > 1) & (spi < -1)] = 2
        cdi[(fapar < -1) & (spi < -1)] = 3
        cdi[(fapar < -1) & (sma > 1) & (spi < -1)] = 4
    else:
        log.warning("Error in calculating SPI-3, SMA or PAR. Cannot calculate CDI!")
        cdi = None
    return cdi


def calcSRI(duration, model, ensemble):
    """Calculate Standardized Runoff Index for specified month
    *duration*."""
    log = logging.getLogger(__name__)
    if bool(ensemble):
        equery = "and (ensemble={0} or ensemble=0)".format(ensemble)
    else:
        equery = ""
    startdate = datetime(model.startyear + model.skipyear, model.startmonth, model.startday)
    enddate = datetime(model.endyear, model.endmonth, model.endday)
    dstartdate = startdate - relativedelta(months=duration)
    if dstartdate > startdate:
        dstartdate = startdate
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select count(fdate) from {0}.runoff where fdate>=date'{1}' and fdate<=date'{2}' {3}".format(model.name, dstartdate.strftime("%Y-%m-%d"), enddate.strftime("%Y-%m-%d"), equery)
    cur.execute(sql)
    nt = cur.fetchone()[0]
    ndays = (enddate - dstartdate).days + 1
    if duration < 1 or (ndays > nt and nt < duration * 30):
        log.warning("Cannot calculate SRI with {0} months duration.".format(duration))
        sri = None
    else:
        db = dbio.connect(model.dbname)
        cur = db.cursor()
        sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.runoff where fdate>=date'{1}' and fdate<=date'{2}' {3} order by fdate".format(model.name, dstartdate.strftime("%Y-%m-%d"), enddate.strftime("%Y-%m-%d"), equery)
        cur.execute(sql)
        results = cur.fetchall()
        data = np.array([np.array(r[1]).ravel() for r in results])
        i = np.where(np.not_equal(data[0, :], None))[0]
        p = pd.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
        t = np.where(p.index == startdate)[0][0]
        pm = p.rolling(duration * 30).mean()  # assume each month is 30 days
        g = [stats.gamma.fit(pm[j][duration * 30:]) for j in pm.columns]
        cdf = np.array([stats.gamma.cdf(pm[j], *g[j]) for j in pm.columns]).T
        sri = np.zeros(cdf.shape)
        sri[duration * 30:, :] = stats.norm.ppf(cdf[duration * 30:, :])
        sri = _clipToValidRange(sri)[t:]
    cur.close()
    db.close()
    return sri


def calcSPI(duration, model, ensemble):
    """Calculate Standardized Precipitation Index for specified month
    *duration*."""
    log = logging.getLogger(__name__)
    if bool(ensemble):
        equery = "and (ensemble={0} or ensemble=0)".format(ensemble)
    else:
        equery = ""
    startdate = datetime(model.startyear + model.skipyear, model.startmonth, model.startday)
    enddate = datetime(model.endyear, model.endmonth, model.endday)
    dstartdate = startdate - relativedelta(months=duration)
    if dstartdate > startdate:
        dstartdate = startdate
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select count(fdate) from {0}.rainf where fdate>=date'{1}' and fdate<=date'{2}' {3}".format(model.name, dstartdate.strftime("%Y-%m-%d"), enddate.strftime("%Y-%m-%d"), equery)
    cur.execute(sql)
    nt = cur.fetchone()[0]
    ndays = (enddate - dstartdate).days + 1
    # tablename = "precip."+model.precip
    if duration < 1 or (ndays > nt and nt < duration * 30):
        log.warning("Cannot calculate SPI with {0} months duration.".format(duration))
        spi = None
    else:
        sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.rainf where fdate>=date'{1}' and fdate<=date'{2}' {3} order by fdate".format(model.name, dstartdate.strftime("%Y-%m-%d"), enddate.strftime("%Y-%m-%d"), equery)
        cur.execute(sql)
        results = cur.fetchall()
        data = np.array([np.array(r[1]).ravel() for r in results])
        i = np.where(np.not_equal(data[0, :], None))[0]
        p = pd.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
        t = np.where(p.index == startdate)[0][0]
        pm = p.rolling(duration * 30).mean()  # assume each month is 30 days
        g = [stats.gamma.fit(pm[j][duration * 30:]) for j in pm.columns]
        cdf = np.array([stats.gamma.cdf(pm[j], *g[j]) for j in pm.columns]).T
        spi = np.zeros(cdf.shape)
        spi[duration * 30:, :] = stats.norm.ppf(cdf[duration * 30:, :])
        spi = _clipToValidRange(spi)[t:]
    cur.close()
    db.close()
    return spi


def calcSeverity(model, ensemble, varname="soil_moist"):
    """Calculate drought severity from *climatology* table stored in database."""
    if bool(ensemble):
        equery = "where (ensemble={0} or ensemble=0)".format(ensemble)
    else:
        equery = ""
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    if varname == "soil_moist":
        sql = "select fdate,(ST_DumpValues(st_union(rast,'sum'))).valarray from {0}.soil_moist {1} group by fdate order by fdate".format(model.name, equery)
    else:
        sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.runoff {1} order by fdate".format(model.name, equery)
    cur.execute(sql)
    results = cur.fetchall()
    data = np.array([np.array(r[1]).ravel() for r in results])
    i = np.where(np.not_equal(data[0, :], None))[0]
    p = pd.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
    p = p.rolling('10D').mean()  # calculate percentiles with dekad rolling mean
    st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
    et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
    s = np.array([[stats.percentileofscore(p[pi].values, v) for v in p[pi][st:et]] for pi in p.columns]).T
    s = 100.0 - s
    cur.close()
    db.close()
    return s


def calcDrySpells(model, ensemble, droughtfun=np.mean, duration=14, recovduration=2):
    """Calculate maps of number of dry spells during simulation period."""
    # FIXME: Currently only uses precipitation to identify dry spells. Need to change it to also use soil moisture and runoff
    if bool(ensemble):
        equery = "and (ensemble={0} or ensemble=0)".format(ensemble)
    else:
        equery = ""
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.rainf where fdate>=date'{1}-{2}-{3}' and fdate<=date'{4}-{5}-{6}' {7} order by fdate".format(model.name, model.startyear, model.startmonth, model.startday, model.endyear, model.endmonth, model.endday, equery)
    cur.execute(sql)
    results = cur.fetchall()
    data = np.array([np.array(r[1]).ravel() for r in results])
    i = np.where(np.not_equal(data[0, :], None))[0]
    p = pd.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
    cur.close()
    db.close()
    ndroughts = np.zeros(p.values.shape)
    for pi in p.columns:
        drought_thresh = droughtfun(p[pi])
        days = 0
        for i in range(recovduration-1, len(p[pi])):
            if p.values[i, pi] <= drought_thresh:
                days += 1
            elif all(p.values[i-j, pi] > drought_thresh for j in range(recovduration)):
                days = 0
            else:
                days += 1
            if days == duration:
                ndroughts[i, pi] = 1
    return np.cumsum(ndroughts, axis=0)


def calcSMDI(model, ensemble):
    """Calculate Soil Moisture Deficit Index (Narasimhan & Srinivasan, 2005)."""
    if bool(ensemble):
        equery = "and (ensemble={0} or ensemble=0)".format(ensemble)
    else:
        equery = ""
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.soil_moist where layer=2 {1} order by fdate".format(model.name, equery)
    cur.execute(sql)
    results = cur.fetchall()
    data = np.array([np.array(r[1]).ravel() for r in results])
    i = np.where(np.not_equal(data[0, :], None))[0]
    clim = pd.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
    st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
    et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
    sw = clim.rolling('7D').mean()
    p = sw[st:et]
    msw = sw.groupby(pd.Grouper(freq='W')).median().reindex(sw.index, method='bfill')
    maxsw = sw.groupby(pd.Grouper(freq='W')).max().reindex(sw.index, method='bfill')
    minsw = sw.groupby(pd.Grouper(freq='W')).min().reindex(sw.index, method='bfill')
    sd = pd.DataFrame(np.where(sw < msw, (sw - msw) / (msw - minsw) * 100, (sw - msw) / (maxsw - msw) * 100), sw.index).fillna(method='ffill')[st:et].values
    smdi = np.zeros(p.shape)
    smdi[0, :] = sd[0, :] / 50
    for t in range(1,smdi.shape[0]):
        smdi[t,:] = 0.5*smdi[t-1, :] + sd[t,:] / 50
    cur.close()
    db.close()
    smdi = np.clip(smdi, -4.0, 4.0)
    return smdi


def calc(varname, model, ensemble):
    """Calculate drought-related variable."""
    if varname.startswith("spi"):
        duration = int(varname[3:])
        output = calcSPI(duration, model, ensemble)
    elif varname.startswith("sri"):
        duration = int(varname[3:])
        output = calcSRI(duration, model, ensemble)
    elif varname == "severity":
        output = calcSeverity(model, ensemble)
    elif varname == "cdi":
        output = calcCDI(model, ensemble)
    elif varname == "smdi":
        output = calcSMDI(model, ensemble)
    elif varname == "dryspells":
        output = calcDrySpells(model, ensemble)
    elif varname == "vci":
        output = calcVCI(model)
    return output
