#!/usr/bin/env python

import numpy as np
import netCDF4
import subprocess
from operator import itemgetter

# read the database of argo profile data as set up
cur_dir = subprocess.check_output("pwd", shell=True)[:-1]
dest_dir = cur_dir + "/profile_pool/"
dbase = np.load(dest_dir + "argo_profile_info_database.npz")

# come up with search criteria/what/who to query (make criteria selection and query below more general)
query = "year"
crit = 2010

# find the entries in database that match search
match = dbase[dbase[query] == crit]

# ideally file list would be only the unique, such that I only open the file
# once and then take all the relevant profiles
daids = list(set(match["floatid"]))  # has to be same as filestoread, could come before pathstofile

# get paths to all the files:
#filestoread = list(set(pathstofiles))  # could come before pathstofile 
#pathstofiles = [dest_dir + m + "_prof.nc" for m in match["floatid"]]
pathstofiles = [dest_dir + m + "_prof.nc" for m in daids]
#prof_idx = match["prof_n"] # maybe deprecated

# ideally file list would be only the unique, such that I only open the file
# once and then take all the relevant profiles
#filestoread = list(set(pathstofiles))  # could come before pathstofile 
#daids = list(set(match["floatid"]))  # has to be same as filestoread, could come before pathstofile

# storage list
argo_profs = []

# read data in the matching lists
#for n in range(0, len(prof_idx)):
for n in range(0, len(filestoread)):
    argo_data = netCDF4.Dataset(pathstofiles[n])
    #  argo_data = netCDF4.Dataset(filestoread[n])
    prof_idxs = match[match["floatid"]==daids[n]]["prof_n"]
    # start pulling off data, should go all into some container class type (which would have limited functionality):
    juld = argo_data.variables['JULD'][prof_idxs]  # at the moment contains multiple profiles
    juld_qc = argo_data.variables['JULD_QC'][prof_idxs]
    juld_loc = argo_data.variables['JULD_LOCATION'][prof_idxs]
    lat = argo_data.variables['LATITUDE'][prof_idxs]
    lon = argo_data.variables['LONGITUDE'][prof_idxs]
    pos_qc = argo_data.variables['POSITION_QC'][prof_idxs]
    prof_pres_qc = argo_data.variables['PROFILE_PRES_QC'][prof_idxs]
    prof_sal_qc = argo_data.variables['PROFILE_PSAL_QC'][prof_idxs]
    prof_temp_qc = argo_data.variables['PROFILE_TEMP_QC'][prof_idxs]
    pres = argo_data.variables['PRES'][prof_idxs, :]
    pres_qc = argo_data.variables['PRES_QC'][prof_idxs, :]
    pres_a = argo_data.variables['PRES_ADJUSTED'][prof_idxs, :]
    pres_a_qc = argo_data.variables['PRES_ADJUSTED_QC'][prof_idxs, :]
    pres_a_er = argo_data.variables['PRES_ADJUSTED_ERROR'][prof_idxs, :]
    sal = argo_data.variables['PSAL'][prof_idxs, :]
    sal_qc = argo_data.variables['PSAL_QC'][prof_idxs, :]
    sal_a = argo_data.variables['PSAL_ADJUSTED'][prof_idxs, :]
    sal_a_qc = argo_data.variables['PSAL_ADJUSTED_QC'][prof_idxs, :]
    sal_a_er = argo_data.variables['PSAL_ADJUSTED_ERROR'][prof_idxs, :]
    temp = argo_data.variables['TEMP'][prof_idxs, :]
    temp_qc = argo_data.variables['TEMP_QC'][prof_idxs, :]
    temp_a = argo_data.variables['TEMP_ADJUSTED'][prof_idxs, :]
    temp_a_qc = argo_data.variables['TEMP_ADJUSTED_QC'][prof_idxs, :]
    temp_a_er = argo_data.variables['TEMP_ADJUSTED_ERROR'][prof_idxs, :]
    max_p = pres.max(axis=-1)
    argo_data.close()
    # storing these data in arrays or lists? something that will allow easy further querying and sorting
    for k in range(0, prof_idxs):
    	argo_profs.append([juld[k], lat[k], lon[k], juld_qc[k], pos_qc[k],
                           max_p[k], prof_pres_qc[k], prof_sal_qc[k],
                           prof_temp_qc[k], pres[k], pres_qc[k], pres_a[k],
                           pres_a_qc[k], pres_a_er[k], sal[k], sal_qc[k],
                           sal_a[k], sal_a_qc[k], sal_a_er[k], temp[k],
                           temp_qc[k], temp_a[k], temp_a_qc[k], temp_a_er[k],])  # is a list of profiles, in fact a list of a list of prof properties
argo_profiles_time = sorted(argo_profs, key=itemgetter(0))  # sort list by time
