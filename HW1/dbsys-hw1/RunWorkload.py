########################################
# This file calls WorkloadGenerator class
# And gives output to stdout
########################################
import sys;
from Utils.WorkloadGenerator import WorkloadGenerator
from Storage.File import StorageFile
from Storage.SlottedPage import SlottedPage
from Storage.PaxPage import PaxPage

if sys.argv[1] == "s":
	StorageFile.defaultPageClass = SlottedPage;
if sys.argv[1] == "p":
	StorageFile.defaultPageClass = PaxPage;

datadir = 'test/datasets/tpch-tiny'
scaleFactor = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
pageSize = [4096, 32768]
workloadMode = [1, 2, 3, 4]

for ps in pageSize:
	for wlm in workloadMode:
		for sf in scaleFactor:
			WorkloadGenerator().runWorkload(datadir, sf, ps, wlm)
			
