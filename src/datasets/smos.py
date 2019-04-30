"""Class definition for the SMOS Soil Mositure data type.

.. module:: smos
   :synopsis: Definition of the SMOS class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from soilmoist import Soilmoist
import dbio
import os
import logging
import pysftp
import re
import tempfile
import netCDF4 as netcdf
from scipy.spatial import KDTree
import numpy as np
from datetime import datetime, timedelta
import datasets


table = "soilmoist.smos"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def regridNearestNeighbor(lat, lon, res):
    """Generate grid of nearest neighbor locations from *lat*, *lon*
    arrays for specified resolution *res*."""
    x, y = np.meshgrid(lon, lat)
    tree = KDTree(zip(x.ravel(), y.ravel()))
    grid_lat = np.arange(round(lat[0], 1), round(lat[-1], 1)-res, -res)
    grid_lon = np.arange(round(lon[0], 1), round(lon[-1], 1)+res, res)
    grid_x, grid_y = np.meshgrid(grid_lon, grid_lat)
    _, pos = tree.query(zip(grid_x.ravel(), grid_y.ravel()))
    return pos, grid_lat, grid_lon


def download(dbname, dts, bbox=None):
    """Downloads SMOS soil mositure data for a set of dates *dt*
    and imports them into the PostGIS database *dbname*. Optionally
    uses a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    log = logging.getLogger(__name__)
    res = 0.25
    username = raw_input("Type in your BEC username to download SMOS data: ")
    password = raw_input("Type in your BEC password to download SMOS data: ")
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection('becftp.icm.csic.es', username=username,
                           password=password, port=27500, cnopts=cnopts) as sftp:
        for dt in [dts[0] + timedelta(ti) for ti in range((dts[-1] - dts[0]).days+1)]:
            filenames = sftp.listdir("data/LAND/SM/SMOS/GLOBAL/v3.0/L3/daily/ASC/{0}".format(dt.year))
            regex = re.compile("BEC.*{0}.*nc".format(dt.strftime("%Y%m%d")))
            filename = [f for f in filenames if regex.match(f)]
            if filename:
                filename = filename[0]
                print(filename)
                datapath = tempfile.mkdtemp()
                os.chdir(datapath)
                sftp.get("data/LAND/SM/SMOS/GLOBAL/v3.0/L3/daily/ASC/{0}/{1}".format(dt.year, filename))
                f = netcdf.Dataset(filename)
                lat = f.variables['lat'][::-1]  # swap latitude orientation to northwards
                lon = f.variables['lon'][:]
                i1, i2, j1, j2 = datasets.spatialSubset(lat, lon, res, bbox)
                smi1 = len(lat) - i2 - 1
                smi2 = len(lat) - i1 - 1
                lat = lat[i1:i2]
                lon = lon[j1:j2]
                t = f.variables['time']
                t = netcdf.num2date(t[-1], t.units)
                sm = f.variables['SM'][-1, smi1:smi2, j1:j2]
                sm = sm[::-1, :]  # flip latitude dimension in data array
                # FIXME: Use spatially variable observation error
                # smv = f.variables['VARIANCE_SM'][0, i1:i2, j1:j2][::-1, :]
                pos, smlat, smlon = regridNearestNeighbor(lat, lon, res)
                smdata = sm.ravel()[pos].reshape((len(smlat), len(smlon)))
                filename = dbio.writeGeotif(smlat, smlon, res, smdata)
                dbio.ingest(dbname, filename, t, table, False)
                os.remove(filename)
            else:
                log.warning("SMOS data not available for {0}. Skipping download!".format(dt.strftime("%Y%m%d")))


class Smos(Soilmoist):

    def __init__(self, uncert=None):
        """Initialize SMOS soil moisture object."""
        super(Smos, self).__init__(uncert)
        self.res = 0.25
        self.stddev = 0.01
        self.tablename = "soilmoist.smos"
