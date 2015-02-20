import io, math, os, os.path, random, time, timeit

from Catalog.Schema        import DBSchema
from Storage.StorageEngine import StorageEngine
from Utils.WorkloadGenerator import *

wg = WorkloadGenerator()
storage = StorageEngine()

wg.createRelations(storage)
sorted(list(storage.relations()))

wg.loadDataset(storage, 'test/datasets/tpch-tiny', 1.0)
print([wg.schemas['nation'].unpack(t).N_NATIONKEY for t in storage.tuples('nation')])

