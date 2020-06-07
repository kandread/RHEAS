""" Class definition for the DSSAT model interface

.. module:: dssat
   :synopsis: Definition of the DSSAT model class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import decimal
import distutils.core
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from .. import dbio


class DSSAT(object):

    def __init__(self, dbname, name, resolution, startyear, startmonth, startday,
                 endyear, endmonth, endday, nens, vicopts, shapefile=None, assimilate=True):
        log = logging.getLogger(__name__)
        self.path = tempfile.mkdtemp(dir=".")
        self.startyear = startyear
        self.startmonth = startmonth
        self.startday = startday
        self.endyear = endyear
        self.endmonth = endmonth
        self.endday = endday
        self.crop = None
        self.cultivars = {}
        self.lat = []
        self.lon = []
        self.elev = []
        self.depths = []
        self.dbname = dbname
        self.name = name
        self.res = resolution
        self.nens = nens
        self.shapefile = shapefile
        self.assimilate = assimilate
        self.data_path = os.path.abspath("../../data")
        self.modelpaths = {}
        self.modelstart = {}
        self.grid_decimal = - (decimal.Decimal(str(self.res)).as_tuple().exponent - 1)
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        if 'lai' in vicopts or ('save' in vicopts and vicopts['save'].find("lai") >= 0):
            self.lai = "vic"
        else:
            self.lai = None
        if 'save to' in vicopts:
            self.datafrom = vicopts['save to']
        else:
            self.datafrom = "db"
        cur.execute(
            "select * from information_schema.tables where table_name='basin' and table_schema=%s", (name,))
        if not bool(cur.rowcount):
            log.error("No simulation named {0} exists in database. You might have to run VIC.".format(name))
            sys.exit()
        cur.execute(
            'select basefile from vic.input where resolution=%f;' % self.res)
        self.basefile = "{0}/{1}".format(self.data_path, cur.fetchone()[0])
        cur.close()
        db.close()

    def readVICSoil(self):
        """Extract information from VIC database table on latitude, longitude,
        elevation  and soil depths."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select st_y(geom), st_x(geom), elev, depths from {0}.basin".format(
            self.name)
        cur.execute(sql)
        pixels = cur.fetchall()
        self.lat, self.lon, self.elev, self.depths = zip(*pixels)
        self.lat = np.array(self.lat)
        self.lon = np.array(self.lon)
        self.elev = np.array(self.elev)
        self.depths = list(self.depths)
        cur.close()
        db.close()

    def writeWeatherFiles(self, modelpath, name, year, month, day, weather, elev, lat, lon, ts=None, te=None):
        """Writes ensemble weather files for specific pixel."""
        if isinstance(weather, list):
            data = (weather * (int(self.nens / len(weather)) + 1))[:self.nens]
        else:
            data = [weather] * self.nens
        for ens in range(self.nens):
            filename = "{0}/WEATH{1:03d}.WTH".format(modelpath, ens + 1)
            fout = open(filename, 'w')
            fout.write("*WEATHER DATA : {0}\r\n".format(name[:5].upper()))
            fout.write("\r\n")
            fout.write("@ INSI LAT LONG ELEV TAV AMP REFHT WNDHT\r\n")
            tavg = np.mean(data[ens][:, 1:3])
            fout.write("{0:6s} {1} {2} {3:.0f} {4:.1f} {5:.1f} {6:.1f} {7:.1f} \r\n".format(
                name[:5].upper(), lat, lon, elev, tavg, -99.0, -99.0, -99.0))
            fout.write("@DATE SRAD TMAX TMIN RAIN DEWP WIND PAR\r\n")
            if ts is None or te is None:
                ts = 0
                te = len(data[ens])
            for p in range(ts, te):
                datestr = str(int(year[p]))[-2:] + date(int(year[p]),
                                                        int(month[p]), int(day[p])).strftime("%j")
                fout.write("{0}  {1:4.1f}  {2:4.1f}  {3:4.1f}  {4:4.1f}\r\n".format(
                    datestr, data[ens][p, 0] * 0.086400, data[ens][p, 1], data[ens][p, 2], data[ens][p, 3]))
            fout.close()

    def readVICOutputFromFile(self, lat, lon, depths, filespath):
        """Read DSSAT inputs from VIC output files for a specific pixel."""
        startdate = date(self.startyear, self.startmonth, self.startday)
        enddate = date(self.endyear, self.endmonth, self.endday)
        filename = "{0}/output/eb_{1:.{3}f}_{2:.{3}f}".format(
            filespath, lat, lon, self.grid_decimal)
        viceb = np.loadtxt(filename)
        filename = "{0}/output/sub_{1:.{3}f}_{2:.{3}f}".format(
            filespath, lat, lon, self.grid_decimal)
        vicsm = np.loadtxt(filename)
        filename = "{0}/output/sur_{1:.{3}f}_{2:.{3}f}".format(
            filespath, lat, lon, self.grid_decimal)
        vicsr = np.loadtxt(filename)
        filename = "{0}/forcings/data_{1:.{3}f}_{2:.{3}f}".format(
            filespath, lat, lon, self.grid_decimal)
        met = np.loadtxt(filename)
        sm = vicsm[:, 3:len(depths) + 3]
        weather = np.vstack(
            (viceb[:, 3] + viceb[:, 4], met[:, 1], met[:, 2], met[:, 0])).T
        year = vicsm[:, 0].astype(int)
        month = vicsm[:, 1].astype(int)
        day = vicsm[:, 2].astype(int)
        tidx = [i for i in range(len(year)) if date(year[i], month[i], day[
            i]) >= startdate and date(year[i], month[i], day[i]) <= enddate]
        lai = dict(zip([date(year[i], month[i], day[i])
                        for i in range(len(year)) if i in tidx], vicsr[:, 12]))
        return year[tidx], month[tidx], day[tidx], weather[tidx, :], sm[tidx, :], lai

    def readVICOutputFromDB(self, gid, depths, planting, harvest):
        """Read DSSAT inputs from database."""
        if planting is None:
            startdate = date(self.startyear, self.startmonth, self.startday)
        else:
            startdate = planting
        if harvest is None:
            enddate = date(self.endyear, self.endmonth, self.endday)
        else:
            enddate = harvest
        ndays = (enddate - startdate).days + 1
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        date_sql = "fdate>=date '{0}' and fdate<=date '{1}'".format(startdate.strftime("%Y-%m-%d"), enddate.strftime("%Y-%m-%d"))
        data = {}
        varnames = ["net_short", "net_long",
                    "soil_moist", "rainf", "tmax", "tmin"]
        if self.lai is not None:
            varnames.append("lai")
        else:
            lai = None
        for varname in varnames:
            sqlvars = ["fdate"]
            sql = "select column_name from information_schema.columns where table_schema='{0}' and table_name='{1}' and column_name='ensemble'".format(
                self.name, varname)
            cur.execute(sql)
            if bool(cur.rowcount):
                sqlvars += ["ensemble"]
            sql = "select column_name from information_schema.columns where table_schema='{0}' and table_name='{1}' and column_name='layer'".format(
                self.name, varname)
            cur.execute(sql)
            if bool(cur.rowcount):
                sqlvars += ["layer"]
            sql = "select {0}, avg((st_summarystats(rast)).mean) from {1}.{2}, {1}.agareas where st_intersects(rast,geom) and gid={3} and {4} group by gid,{0} order by fdate".format(",".join(sqlvars), self.name, varname, gid, date_sql)
            cur.execute(sql)
            if bool(cur.rowcount):
                results = cur.fetchall()
                if "ensemble" in sqlvars:
                    vicnens = np.max([r[1] for r in results])
                    data[varname] = [np.array(
                        [r[-1] for r in results if r[1] == ens + 1 or r[1] == 0]) for ens in range(vicnens)]
                    if "layer" in sqlvars:
                        layers = np.array([r[2] for r in results if r[1] == 1 or r[1] == 0])
                        nlayers = np.max(layers)
                    else:
                        year = np.array([r[0].year for r in results if r[1] == 1 or r[1] == 0])
                        month = np.array([r[0].month for r in results if r[1] == 1 or r[1] == 0])
                        day = np.array([r[0].day for r in results if r[1] == 1 or r[1] == 0])
                else:
                    data[varname] = np.array([r[-1] for r in results])
                    if "layer" in sqlvars:
                        layers = np.array([r[1] for r in results])
                        nlayers = np.max(layers)
                    else:
                        year = np.array([r[0].year for r in results])
                        month = np.array([r[0].month for r in results])
                        day = np.array([r[0].day for r in results])
                # assert len(year) == ndays and len(month) == ndays and len(day) == ndays
        cur.close()
        db.close()
        if "ensemble" in sqlvars:
            weather = [np.vstack((data["net_short"][e] - data["net_long"][e], data["tmax"][
                                 e], data["tmin"][e], data["rainf"][e])).T for e in range(len(data["net_short"]))]
            sm = [np.zeros((len(year), nlayers))] * len(data["soil_moist"])
            if self.lai is not None:
                lai = dict(zip([date(year[i], month[i], day[i]) for i in range(
                    len(year))], np.mean(np.array(data["lai"]).T, axis=1)))
            for e in range(len(sm)):
                for l in range(nlayers):
                    sm[e][:, l] = [m for mi, m in enumerate(
                        data["soil_moist"][e]) if layers[mi] == l + 1]
        else:
            weather = np.vstack(
                (data["net_short"] - data["net_long"], data["tmax"], data["tmin"], data["rainf"])).T
            if self.lai is not None:
                lai = dict(zip([date(year[i], month[i], day[i])
                                for i in range(len(year))], np.array(data["lai"]).T))
            sm = np.zeros((len(year), nlayers))
            for l in range(nlayers):
                sm[:, l] = [m for mi, m in enumerate(
                    data["soil_moist"]) if layers[mi] == l + 1]
        return year, month, day, weather, sm, lai

    def readVICOutput(self, gid, depths, planting=None, harvest=None):
        """Reads DSSAT time-varying inputs by reading either from files or a database."""
        log = logging.getLogger(__name__)
        if isinstance(self.datafrom, list):
            inputs = []
            while len(inputs) < self.nens:
                inputs += self.datafrom
            inputs = inputs[:self.nens]
            lat, lon = self.gid[gid]
        if self.datafrom == 'db':
            year, month, day, weather, sm, lai = self.readVICOutputFromDB(
                gid, depths, planting, harvest)
        else:
            log.error("VIC output was not saved in the database. Cannot proceed with the DSSAT simulation.")
            sys.exit()
        return year, month, day, weather, sm, lai

    def writeLAI(self, modelpath, gid, year, month, day, ts=None, te=None, viclai=None, tablename="lai.modis"):
        """Writes LAI file for DSSAT."""
        fout = open("{0}/LAI.txt".format(modelpath), 'w')
        if ts is None or te is None:
            ts = 0
            te = len(year)
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute("select * from information_schema.tables where table_name=%s and table_schema='lai'",
                    (tablename.split(".")[1],))
        if bool(cur.rowcount) and not self.lai == "vic":
            dt1 = date(year[ts], month[ts], day[ts])
            dt2 = date(year[te-1], month[te-1], day[te-1])
            sql = "select fdate,avg((st_summarystats(st_clip(rast,geom))).mean) from {0},{1}.agareas where st_intersects(rast,geom) and fdate>=date '{2}' and fdate<=date '{3}' and gid={4} group by fdate".format(tablename, self.name, dt1.strftime("%Y-%m-%d"), dt2.strftime("%Y-%m-%d"), gid)
            cur.execute(sql)
            if bool(cur.rowcount):
                results = cur.fetchall()
                lai = {}
                for r in results:
                    if r[1] is None:
                        lai[r[0]] = -9999.0
                    else:
                        lai[r[0]] = r[1] / 10.0
            else:
                lai = {}
        else:
            lai = viclai
        for t in range(ts, te):
            dt = date(year[t], month[t], day[t])
            if lai is not None and dt in lai:
                fout.write("{0:.1f}\n".format(lai[dt]))
            else:
                fout.write("-9999.0\n")
        fout.close()
        cur.close()
        db.close()

    def writeSoilMoist(self, modelpath, year, month, day, smi, dz, ts=None, te=None):
        """Writes soil moisture information file."""
        filename = "{0}/SOIL_MOISTURE.ASC".format(modelpath)
        fout = open(filename, 'w')
        if ts is None or te is None:
            ts = 0
            te = len(smi)
        for t in range(ts, te):
            dt = date(year[t], month[t], day[t])
            doy = int(dt.strftime("%j"))
            fout.write("{0:.0f} {1:.0f} {2:.0f} ".format(
                dt.year, dt.month, dt.day))
            for lyr in range(len(dz)):
                fout.write("{0:.3f} ".format(smi[t, lyr]))
            fout.write("{0}\n".format(doy))
        fout.close()

    def sampleSoilProfiles(self, gid):
        """Samples soil profiles from database to be used in DSSAT control file."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "with f as (select st_envelope(geom) as geom from {0}.agareas where gid={1}) select props from dssat.soils as s,f where st_intersects(s.geom,f.geom)".format(self.name, gid)
        cur.execute(sql)
        # if crop area is too small, look for nearest soil profiles
        dist = 0.1
        while not bool(cur.rowcount):
            sql = "with a as (select st_buffer(geom,{2}) as geom from {0}.agareas where gid={1}) select props from dssat.soils as s,a where st_intersects(s.geom,a.geom)".format(
                self.name, gid, dist)
            dist += 0.1
            cur.execute(sql)
        profiles = cur.fetchall()
        ens = np.random.choice(range(len(profiles)), self.nens)
        cur.close()
        db.close()
        return [profiles[e] for e in ens]

    def writeConfigFile(self, modelpath, nlayers, startdate, enddate):
        """Write DSSAT-ENKF config file."""
        configfilename = "ENKF_CONFIG.TXT"
        fout = open("{0}/{1}".format(modelpath, configfilename), 'w')
        fout.write("!Start_DOY_of_Simulation:\n{0}\n".format(
            int(startdate.strftime("%j"))))
        fout.write("!End_DOY_of_Simulation\n{0}\n".format(
            int(enddate.strftime("%j"))-1))
        fout.write("!Year_of_Simulation:\n{0}\n".format(startdate.year))
        fout.write("!Ensemble_members\n{0}\n".format(self.nens))
        fout.write("!Number_of_soil_layers\n{0}\n".format(nlayers))
        ndays = (enddate - startdate).days
        fout.write("!Number_of_RS_data\n{0}".format(ndays))
        fout.close()
        return configfilename

    def calcCroplandFract(self):
        """Calculate fraction of cropland for specific pixel."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select gid,avg((st_summarystats(st_clip(rast,geom))).mean) from dssat.cropland,{0}.agareas where st_intersects(rast,geom) group by gid order by gid".format(
            self.name)
        cur.execute(sql)
        fract = dict((r[0], r[1]) for r in cur.fetchall())
        cur.close()
        db.close()
        return fract

    def readShapefile(self):
        """Read areas from shapefile where DSSAT will be run."""
        log = logging.getLogger(__name__)
        try:
            cmd = "shp2pgsql -s 4326 -d -I -g geom {0} {1}.agareas | psql -d {2}".format(self.shapefile, self.name, self.dbname)
            subprocess.call(cmd, shell=True)
            db = dbio.connect(self.dbname)
            cur = db.cursor()
            sql = "select gid, st_x(st_centroid(geom)), st_y(st_centroid(geom)) from {0}.agareas".format(self.name)
            cur.execute(sql)
            geoms = cur.fetchall()
            return geoms
        except IOError:
            log.error("Shapefile {0} for DSSAT simulation does not exist. Exiting...".format(
                self.shapefile))
            sys.exit()

    def planting(self, lat, lon, fromShapefile=False):
        """Retrieve planting dates for pixel."""
        if self.crop is None:
            self.crop = "maize"
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select st_nearestvalue(rast,st_geomfromtext('POINT({0} {1})',4326)) as doy from crops.plantstart where type like '{2}' and st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)) order by doy".format(
            lon, lat, self.crop)
        cur.execute(sql)
        results = cur.fetchall()
        # if this is a multi-year simulation append planting dates for each successive year
        plantdates = []
        for yr in range(self.startyear, self.endyear + 1):
            for r in results:
                if r[0] is not None:
                    plantdates.append(date(yr, 1, 1) + timedelta(r[0] - 1))
        cur.close()
        db.close()
        startdt = date(self.startyear, self.startmonth, self.startday)
        planting = [p for p in plantdates if p >= startdt and p <= date(self.endyear, self.endmonth, self.endday)]
        # with the exception of no planting dates satisfying that condition (e.g. short-term forecasts)
        # in that case, we will find the closest past date to the simulation start date
        aplant = []
        sdt = int(startdt.strftime('%j'))
        for r in results:
            if r[0] is not None:
                if r[0] > sdt:
                    aplant.append(date(startdt.year-1, 1, 1) + timedelta(r[0] - 1))
                else:
                    aplant.append(date(startdt.year, 1, 1) + timedelta(r[0] - 1))
        planting = [max(aplant)] + planting
        return planting

    def interpolateSoilMoist(self, sm, depths, dz):
        """Estimate soil moisture at DSSAT depths."""
        sm_i = []
        if len(sm.shape) < 2:
            sm = np.reshape(sm, (1, len(sm)))
        for t in range(sm.shape[0]):
            u = sm[t, :] / np.array(depths * 1000.0)
            z = [100.0 * depths[0] / 2.0]
            for lyr in range(1, len(u)):
                # midpoint of each layer in cm
                z.append(100.0 * (depths[lyr - 1] + depths[lyr] / 2.0))
            dz1 = [0.0] + list(dz)
            znew = np.array([dz1[i] + (dz1[i + 1] - dz1[i]) /
                             2.0 for i in range(len(dz1) - 1)])
            unew = np.interp(znew, z, u)
            sm_i.append(unew)
        return np.array(sm_i)

    def copyModelFiles(self, geom, pi, dssatexe):
        """Copy DSSAT model files to instance's directory."""
        gid, lat, lon = geom
        modelpath = os.path.abspath("{0}/{1}_{2}_{3}".format(self.path, lat, lon, pi))
        self.modelpaths[(gid, pi)] = modelpath
        os.mkdir(modelpath)
        os.mkdir(modelpath + "/ENKF_Results")
        shutil.copyfile("{0}/{1}".format(os.path.abspath("../../external/dssat"), dssatexe), "{0}/{1}".format(modelpath, dssatexe))
        distutils.dir_util.copy_tree("{0}/dssat".format(self.data_path), modelpath)

    def setupModelInstance(self, geom, dssatexe):
        """Setup parameters and write input files for a DSSAT model instance
        over a specific geometry."""
        log = logging.getLogger(__name__)
        gid, lon, lat = geom
        c = np.argmin(np.sqrt((lat - self.lat) **
                              2 + (lon - self.lon) ** 2))
        # use the soil depths from the nearest VIC pixel to the centroid
        depths = np.array(self.depths[c])
        planting = self.planting(lat, lon)
        # FIXME: instead of 180 days perhaps use information from harvest dates to define the simulation end date?
        harvest_days = 180
        # we will start the simulation 7 days before the first planting date if possible
        simstartdt = planting[0] - timedelta(7)
        year, month, day, weather, sm, vlai = self.readVICOutput(gid, depths, simstartdt, planting[-1] + timedelta(harvest_days))
        vicstartdt = date(year[0], month[0], day[0])
        for pi, pdt in enumerate(planting):
            if (pdt - vicstartdt).days < 0:
                log.warning("Cannot perform simulation for planting date {0}. Earliest available VIC output is on {1}".format(pdt.strftime("%Y-%m-%d"), vicstartdt.strftime("%Y-%m-%d")))
            else:
                simstartdt = max(vicstartdt, pdt - timedelta(7))
                self.copyModelFiles(geom, pi, dssatexe)
                modelpath = self.modelpaths[(gid, pi)]
                self.modelstart[(gid, pi)] = simstartdt
                dz, smi = self.writeControlFile(modelpath, sm, depths, simstartdt, gid, self.lat[c], self.lon[c], pdt, None, None)
                ti0 = [i for i in range(len(year)) if simstartdt == date(year[i], month[i], day[i])][0]
                ti1 = ti0 + harvest_days
                if ti1 > len(year):
                    log.warning("Inadequate record legnth in VIC data to ensure harvest for {0} planting date! Plant will not reach maturity and yield values will be invalid. Please extent VIC simulation to at least {1}!".format(pdt.strftime("%Y-%m-%d"), (pdt + timedelta(harvest_days)).strftime("%Y-%m-%d")))
                    ti1 = len(year) - 1
                self.writeWeatherFiles(modelpath, self.name, year, month, day, weather, self.elev[c], self.lat[c], self.lon[c], ti0, ti1)
                self.writeSoilMoist(modelpath, year, month, day, smi, dz, ti0, ti1)
                self.writeLAI(modelpath, gid, year, month, day, ti0, ti1, viclai=vlai)
                self.writeConfigFile(modelpath, smi.shape[1], simstartdt, date(year[ti1], month[ti1], day[ti1]))
                log.info("Wrote DSSAT for planting date {0}".format(pdt.strftime("%Y-%m-%d")))

    def runModelInstance(self, modelpath, dssatexe):
        """Runs DSSAT model instance."""
        log = logging.getLogger(__name__)
        os.chdir(modelpath)
        if bool(self.assimilate):
            if str(self.assimilate).lower() is "sm":
                sm_assim = "Y"
                lai_assim = "N"
            elif str(self.assimilate).lower() is "lai":
                sm_assim = "N"
                lai_assim = "Y"
            else:
                sm_assim = lai_assim = "Y"
        else:
            sm_assim = lai_assim = "N"
        proc = subprocess.Popen(["wine", dssatexe, "SOIL_MOISTURE.ASC", "LAI.txt", "SM{0}".format(sm_assim), "LAI{0}".format(lai_assim)])
        out, err = proc.communicate()
        log.debug(out)

    def save(self):
        """Saves DSSAT output to database."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute(
            "select * from information_schema.tables where table_name='dssat' and table_schema='{0}'".format(self.name))
        if not bool(cur.rowcount):
            cur.execute("create table {0}.dssat (id serial primary key, gid int, ensemble int, harvest date, planting date, wsgd real, lai real, gwad real, geom geometry, CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2), CONSTRAINT enforce_geotype_geom CHECK (geometrytype(geom) = 'POLYGON'::text OR geometrytype(geom) = 'MULTIPOLYGON'::text OR geom IS NULL))".format(self.name))
            db.commit()
        # overwrite overlapping dates
        cur.execute("delete from {0}.dssat where planting>=date'{1}-{2}-{3}' and planting<=date'{4}-{5}-{6}'".format(self.name, self.startyear, self.startmonth, self.startday, self.endyear, self.endmonth, self.endday))
        sql = "insert into {0}.dssat (planting, harvest, gid, ensemble, gwad, wsgd, lai) values (%(pdt)s, %(hdt)s, %(gid)s, %(ens)s, %(gwad)s, %(wsgd)s, %(lai)s)".format(self.name)
        for gid, pi in self.modelpaths:
            modelpath = self.modelpaths[(gid, pi)]
            for e in range(self.nens):
                data = pd.read_csv("{0}/PLANTGRO{1:03d}.OUT".format(modelpath, e + 1), delim_whitespace=True)
                hvst = data.index[-1]  # assume that harvest occurred at last date of simulation
                plnt = data.index[0]
                year = data['@YEAR']
                doy = data['DOY']
                lai = data['LAID'][hvst]
                gwad = data['GWAD'][hvst]
                wsgd = data['WSGD'][hvst]
                hdts = datetime.strptime("{0:04d}{1:03d}".format(year[hvst], doy[hvst]), "%Y%j").strftime("%Y-%m-%d")
                pdts = datetime.strptime("{0:04d}{1:03d}".format(year[plnt], doy[plnt]), "%Y%j").strftime("%Y-%m-%d")
                if self.cultivars[gid][e] is None:
                    cultivar = ""
                else:
                    cultivar = self.cultivars[gid][e]
                if gwad > 0:
                    cur.execute(sql, {'pdt': pdts, 'hdt': hdts, 'ens': e + 1, 'gwad': gwad, 'wsgd': wsgd, 'lai': lai, 'gid': gid, 'cultivar': cultivar})
        cur.execute(
            "update {0}.dssat as d set geom = a.geom from {0}.agareas as a where a.gid=d.gid".format(self.name))
        db.commit()
        cur.execute("drop index if exists {0}.d_t".format(self.name))
        cur.execute("drop index if exists {0}.d_s".format(self.name))
        cur.execute(
            "create index d_t on {0}.dssat(planting)".format(self.name))
        cur.execute(
            "create index d_s on {0}.dssat using gist(geom)".format(self.name))
        db.commit()
        cur.close()
        db.close()
        self.yieldTable()

    def yieldTable(self):
        """Create table for crop yield statistics."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        if dbio.tableExists(self.dbname, self.name, "yield"):
            cur.execute("drop table {0}.yield".format(self.name))
        sql = "create table {0}.yield as (select gid, geom, planting, min(harvest) as first_harvest, max(harvest) as last_harvest, avg(gwad) as avg_yield, max(gwad) as max_yield, min(gwad) as min_yield, stddev(gwad) as std_yield from {0}.dssat group by gid,geom,planting)".format(self.name)
        cur.execute(sql)
        cur.execute("alter table {0}.yield add column crop text".format(self.name))
        cur.execute("drop index if exists {0}.yield_s".format(self.name))
        db.commit()
        cur.execute("create index yield_s on {0}.yield using gist(geom)".format(self.name))
        cur.close()
        db.close()

    def run(self, dssatexe="DSSAT_EnKF.exe", crop_threshold=0.1):
        """Runs DSSAT simulation."""
        if self.crop:
            dssatexe = "DSSAT_EnKF_{0}.exe".format(self.crop)
        self.readVICSoil()
        geoms = self.readShapefile()
        cropfract = self.calcCroplandFract()
        for geom in geoms:
            gid = geom[0]
            if cropfract[gid] >= crop_threshold:
                self.setupModelInstance(geom, dssatexe)
        pwd = os.getcwd()
        for k in self.modelpaths:
            modelpath = self.modelpaths[k]
            self.runModelInstance(modelpath, dssatexe)
        self.save()
        os.chdir(pwd)
