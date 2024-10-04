from TopoPyScale import topoclass as tc
import os
import sys


mydir = sys.argv[1]
os.chdir(mydir)
config_file = './config.yml'
mp  = tc.Topoclass(config_file)
mp.compute_dem_param()
mp.extract_topo_param() # this loads existing datasets
mp.toposub.write_landform()
