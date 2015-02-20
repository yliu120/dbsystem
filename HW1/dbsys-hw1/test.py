import io, math, os, os.path, random, time, timeit
import Storage.PaxPage
from Catalog.Identifiers   import *
from Storage.StorageEngine import StorageEngine
from Utils.WorkloadGenerator import *
from Storage.File import *
from Storage.SlottedPage import SlottedPageHeader, SlottedPage
from Storage.PaxPage import PaxPage, PaxPageHeader
from Catalog.Schema import *

StorageFile.defaultPageClass = SlottedPage

wg = WorkloadGenerator()
storage = StorageEngine()

wg.createRelations(storage)
sorted(list(storage.relations()))

wg.loadDataset(storage, 'test/datasets/tpch-tiny', 1.0)
print(wg.schemas['nation'].size)
print(wg.schemas['supplier'].size)
print([wg.schemas['supplier'].unpack(t).S_SUPPKEY for t in storage.tuples('supplier')])


