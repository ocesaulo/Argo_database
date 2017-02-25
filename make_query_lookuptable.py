#!/usr/bin/env python

'''
 assembles a database of ARGO float profile data downloaded from GDAC
 prepare look-up table array/list/dict
 must contain EVERY PROFILE or EVERY FLOAT
 each profile is a line in the dbase list
 columns have the relevant profile info
 the order of floats and profiles does not matter
'''

import argparse
import numpy as np
import glob
import os
import netCDF4
import datetime
from matplotlib.dates import num2date, datestr2num
import subprocess

# usually 1st is good to pool all profile float files together as symlinks
# this assumes the typical float file/dir structure after pull down from server

parser = argparse.ArgumentParser(description="setup ARGO profile lookup database")
parser.add_argument("source_dir", type=str, help="full path to directory containing source data (download folder)")
parser.add_argument("dest_dir", type=str, nargs='?', help="directory path where data links and array will reside")
args = parser.parse_args()

cur_dir = subprocess.check_output("pwd", shell=True)[:-1]

print "source dir is " + args.source_dir
source_dir = args.source_dir  # dir of source data (nc files)
# source_dir = cur_dir + "/testdata/"  # dir of source data (nc files)

if args.dest_dir:
   print "dest dir is " + args.dest_dir
   dest_dir = args.dest_dir
else:
   print "creating profile_pool dir in current dir"
   dest_dir = cur_dir + "/profile_pool/"  # where to put database and links

if not os.path.isdir(dest_dir):
    os.system("mkdir " + dest_dir)
    print "creating destination directory"

if not os.listdir(dest_dir):
    os.system("ln -s " + source_dir + "*/*/*prof.nc " + dest_dir)
    print "creating sym links to raw argo profile netcdf data"
else:
    print "There is stuff in the destination directory, assuming its the data"

# use glob to form a list of input files

prof_files = glob.glob(dest_dir + '*.nc')
# prof_files.sort(key=lambda x: [int(x.split('-')[2])])  # no need for sorting

# prepare look-up table array/list/dict
# maybe list less ideal because it's slow and lists may take up more memory to fill 
dbase = []  # dbase is the list of profiles that contains profile info

# loop over input files, retrieve the necessary info and store it in the
# appropriate place in
print "putting together database: list filling loop"
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
        pmin = prof_data.variables["PRES"][n].min()
        pmax = prof_data.variables["PRES"][n].max()
        p_pres_qc = prof_data.variables["PROFILE_PRES_QC"][n]
        p_sal_qc = prof_data.variables["PROFILE_PSAL_QC"][n]
        p_temp_qc = prof_data.variables["PROFILE_TEMP_QC"][n]
        jul_qc = prof_data.variables["JULD_QC"][n]
        pos_qc = prof_data.variables["POSITION_QC"][n]
        dbase.append( (floatid, nprofs, nlevs, year, mon, day, juld, lat, lon,
                       pmin, pmax, jul_qc, pos_qc, p_pres_qc, p_sal_qc, p_temp_qc, n) )
dbase = np.array(dbase, dtype=[("floatid", '|S21'), ('nprofs', 'int32'),
                               ('nlevs', 'int32'), ('year', 'int32'),
                               ('month', 'int32'), ('day', 'int32'),
                               ('juld', 'float32'), ('lat', 'float32'),
                               ('lon', 'float32'), ('pmin', 'float32'),
                               ('pmax', 'float32'), ('jul_qc', 'int32'),
                               ('pos_qc', 'int32'), ("pp_qc", '|S21'),
                               ("ps_qc", '|S21'), ("pt_qc", '|S21'), ('prof_n', 'int32')])
np.savez_compressed(dest_dir + "argo_profile_info_database", dbase=dbase)
