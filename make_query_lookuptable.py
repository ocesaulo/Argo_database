#!/usr/bin/env python

# assembles a database of ARGO float profile data downloaded from GDAC
# prepare look-up table array/list/dict
# must contain EVERY PROFILE or EVERY FLOAT
# each profile is a line in the dbase list
# columns have the relevant profile info
# the order of floats and profiles does not matter

import numpy as np
import glob
import os
import netCDF4
import datetime
from matplotlib.dates import num2date, datestr2num
import subprocess

# usually 1st is good to pool all profile float files together as symlinks
# this assumes the typical float file/dir structure after pull down from server
# would be good to turn these directories into argument inputs to the script

cur_dir = subprocess.check_output("pwd", shell=True)[:-1]
source_dir = cur_dir + "/testdata/"  # dir of source data (nc files)
dest_dir = cur_dir + "/profile_pool/"  # where to put database and links

if not os.path.isdir(dest_dir):
    os.system("mkdir " + dest_dir)
    print "creating destination directory"

if not os.listdir(dest_dir):
    os.system("ln -s " + source_dir + "*/*prof.nc " + dest_dir)
    print "creating sym links to raw argo profile netcdf data"
else:
    print "There is stuff in the destination directory, assuming its the data"

# use glob to form a list of input files

prof_files = glob.glob(dest_dir + '*.nc')
# prof_files.sort(key=lambda x: [int(x.split('-')[2])])  # no need for sorting

# prepare look-up table array/list/dict
 
dbase = []  # dbase is the list of profiles that contains profile info

# loop over input files, retrieve the necessary info and store it in the
# appropriate place in
for dafile in prof_files:
    prof_data = netCDF4.Dataset(dafile)
    nprofs = prof_data.dimensions["N_PROF"].size
    nlevs = prof_data.dimensions["N_LEVELS"].size
    try:
        floatid = "".join(prof_data.variables["PLATFORM_NUMBER"][0,:].compressed())
    except:
        floatid = "".join(prof_data.variables["PLATFORM_NUMBER"][0,:])
    ref_date = datetime.datetime.strptime("".join(prof_data.variables["REFERENCE_DATE_TIME"]), "%Y%m%d%H%M%S")
    ref_date_num = datestr2num(datetime.datetime.strftime(ref_date, "%Y/%m/%d")) 
    for n in range(0, nprofs):
        juld = prof_data.variables["JULD"][n]
        date_prof = num2date(ref_date_num + juld)  # will be UTC
        year, mon, day = date_prof.year, date_prof.month, date_prof.day
        lat = prof_data.variables["LATITUDE"][n]
        lon = prof_data.variables["LONGITUDE"][n]
        pmin = prof_data.variables["PRESSURE"][n].min()
        pmax = prof_data.variables["PRESSURE"][n].max()
        jul_qc = prof_data.variables["JULD_QC"][n]
        pos_qc = prof_data.variables["POSITION_QC"][n]
        dbase.append( (floatid, nprofs, nlevs, year, mon, day, juld, lat, lon, pmin, pmax, jul_qc, pos_qc, n) )
dbase = np.array(dbase, dtype=[("floatid", '|S21'), ('nprofs', 'int32'),
                               ('nlevs', 'int32'), ('year', 'int32'),
                               ('month', 'int32'), ('day', 'int32'),
                               ('juld', 'float32'), ('lat', 'float32'),
                               ('lon', 'float32'), ('pmin', 'float32'),
                               ('pmax', 'float32'), ('jul_qc', 'int32'),
                               ('pos_qc', 'int32'), ('prof_n', 'int32')])
np.savez(dest_dir + "argo_profile_info_database", dbase=dbase)
