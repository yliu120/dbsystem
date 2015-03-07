import Database
import sys
from collections import deque

from Catalog.Schema  import DBSchema

from Query.Operators.TableScan import TableScan
from Query.Operators.Select    import Select
from Query.Operators.Project   import Project
from Query.Operators.GroupBy   import GroupBy
import random


db = Database.Database()
db.createRelation('employee', [('id', 'int'), ('age', 'int')])
schema = db.relationSchema('employee')

for tup in [schema.pack(schema.instantiate(i, random.randint(0,50))) for i in range(0,20000)]:
  _ = db.insertTuple(schema.name, tup)

keySchema  = DBSchema('employeeKey',  [('id', 'int')])
aggMinMaxSchema = DBSchema('minmax', [('minAge', 'int'), ('maxAge','int')])

'''
query6 = db.query().fromTable('employee').groupBy( \
          groupSchema=keySchema, \
          aggSchema=aggMinMaxSchema, \
          groupExpr=(lambda e: e.id), \
          aggExprs=[(sys.maxsize, lambda acc, e: min(acc, e.age), lambda x: x), \
                    (0, lambda acc, e: max(acc, e.age), lambda x: x)], groupHashFn=(lambda gbVal: gbVal % 4)).finalize()

print(query6.explain())
q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]

print(q6results);
print( len(q6results) );
'''
fileIterator = db.storage.fileMgr.relationFile('employee')[1].pages();

for page in fileIterator:
  print(sorted( iter(page[1]), key=lambda tuple : schema.unpack(tuple).age )[0]);

