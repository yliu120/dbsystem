########################################
# This file calls WorkloadGenerator class
# And gives output to stdout
########################################

from Utils.WorkloadGenerator import WorkloadGenerator
import numpy as np

datadir = 'test/datasets/tpch-tiny'
scaleFactor = np.arange(0.0, 1.1, 0.1)
# scaleFactor = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
pageSize = [4096, 32768]
workloadMode = [1, 2, 3, 4]

for ps in pageSize:
	for wlm in workloadMode:
		for sf in scaleFactor:
			WorkloadGenerator().runWorkload(datadir, sf, ps, wlm)
			
