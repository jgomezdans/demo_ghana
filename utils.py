#!/usr/bin/env python
import json

import datetime as dt

import urllib.request

import numpy as np

import gdal
gdal.UseExceptions()

def extract_roi_data_ndre(img_db, roi=None, b0=3, b1=6, local_file=None):
    analysis_data = {}
    for k in img_db.keys():
        if local_file is None:
            g = gdal.Warp("", f"/vsicurl/{img_db[k][b0]:s}", format="MEM",
                     cutlineDSName=f"/vsicurl/{roi}", cropToCutline=True)
        else:
            g = gdal.Warp("", f"/vsicurl/{img_db[k][b0]:s}", format="MEM",
                     cutlineDSName=f"{local_file}", cropToCutline=True)
        data1 = g.ReadAsArray()*1.
        if local_file is None:
            g = gdal.Warp("", f"/vsicurl/{img_db[k][b1]:s}", format="MEM",
                     cutlineDSName=f"/vsicurl/{roi}", cropToCutline=True)
        else:
            g = gdal.Warp("", f"/vsicurl/{img_db[k][b1]:s}", format="MEM",
                     cutlineDSName=f"{local_file}", cropToCutline=True)
        data2 = g.ReadAsArray()*1.
        data1[data1 < -9990] = np.nan
        data2[data2 < -9990] = np.nan
        data1 = data1/10000.
        data2 = data2/10000.
        if not np.isnan(np.nanmean(data1)):
            print(f"There is data for {k.strftime('%d %B %Y'):s}")
            ndvi = (data2-data1)/(data2+data1)
            analysis_data[k] = ndvi
    return analysis_data




def grab_holdings(url="http://www2.geog.ucl.ac.uk/" +
                  "~ucfajlg/Ghana/composites/database.json"):

    tmp_db  = json.loads(urllib.request.urlopen(url).read())
    dates = [(k, dt.datetime.strptime(k, "%Y-%m-%d").date())
             for k in tmp_db.keys()]

    the_db = {kk:tmp_db[k] for (k,kk) in dates}
    return the_db
