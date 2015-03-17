import Database
import sys
from collections import deque

from Catalog.Schema  import DBSchema

from Query.Operators.TableScan import TableScan
from Query.Operators.Select    import Select
from Query.Operators.Project   import Project
from Query.Operators.Join   import Join
import random


db = Database.Database(dataDir="data2")
db.createRelation('employeeeee', [('id', 'int'), ('age', 'int'), ('desc', 'char(40)')])
db.createRelation('employeeeee2', [('id2', 'int'), ('age2', 'int'), ('desc2', 'char(40)')])
schema1 = db.relationSchema('employeeeee')
schema2 = db.relationSchema('employeeeee2')

for tup in [schema1.pack(schema1.instantiate(i, random.randint(0,50), 'This is a testing tuple.')) for i in range(0,10000)]:
  _ = db.insertTuple(schema1.name, tup)

for tup in [schema2.pack(schema2.instantiate(i, random.randint(0,50), 'This is a testing tuple.')) for i in range(0,10000)]:
  _ = db.insertTuple(schema2.name, tup)

keySchema  = DBSchema('employeeKey',  [('id', 'int')])
keySchema2 = DBSchema('employeeKey2', [('id2', 'int')])
  
query5 = db.query().fromTable('employeeeee').join( \
          db.query().fromTable('employeeeee2'), \
          rhsSchema=schema2, \
          method='hash', \
          lhsHashFn=(lambda e : e.id % 4),  lhsKeySchema=keySchema, \
          rhsHashFn=(lambda e : e.id2 % 4), rhsKeySchema=keySchema2, \
        ).finalize()

print(query5.explain());

def readResult():
    for page in db.processQuery(query5):
        for tup in page[1]:
            yield query5.schema().unpack(tup);

q5result = readResult();

while( q5result ):
    print(next(q5result));