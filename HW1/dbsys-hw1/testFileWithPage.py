import io, math, os, os.path, pickle, struct
from struct import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema
from Storage.Page        import PageHeader, Page
from Storage.SlottedPage import SlottedPageHeader, SlottedPage

from Storage.File import *

import shutil, Storage.BufferPool, Storage.FileManager

schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])

bp = Storage.BufferPool.BufferPool()

fm = Storage.FileManager.FileManager(bufferPool=bp)

bp.setFileManager(fm)

fm.createRelation(schema.name, schema)

(fId, f) = fm.relationFile(schema.name)

f.numPages() == 0

pId  = PageId(fId, 0)
pId1 = PageId(fId, 1)

p    = Page(pageId=pId,  buffer=bytes(f.pageSize()), schema=schema)
p1   = Page(pageId=pId1, buffer=bytes(f.pageSize()), schema=schema)

for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  _ = p.insertTuple(tup)

for tup in [schema.pack(schema.instantiate(i, i+20)) for i in range(10, 20)]:
  _ = p1.insertTuple(tup)

f.writePage(p)
f.writePage(p1)
print(p.header.usedSpace())
h1 = f.readPageHeader( pId );
print(h1)
print(h1.tupleSize)
print(h1.freeSpaceOffset)
print(h1.pageCapacity)
print(h1.usedSpace())
print(f.numPages() == 2)

print([p[1].usedSpace() for p in f.headers()])
print([p[1].pageId.pageIndex for p in f.pages()])
