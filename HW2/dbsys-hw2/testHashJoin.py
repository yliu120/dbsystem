import Database
import sys
from collections import deque

from Catalog.Schema  import DBSchema

from Query.Operators.TableScan import TableScan
from Query.Operators.Select    import Select
from Query.Operators.Project   import Project
from Query.Operators.Join   import Join
import random


db = Database.Database()
db.createRelation('employee', [('id', 'int'), ('age', 'int'), ('desc', 'char(40)')])
db.createRelation('employee2', [('id2', 'int'), ('age2', 'int'), ('desc2', 'char(40)')])
schema1 = db.relationSchema('employee')
schema2 = db.relationSchema('employee2')

for tup in [schema1.pack(schema1.instantiate(i, random.randint(0,50), 'This is a testing tuple.')) for i in range(0,2000)]:
  _ = db.insertTuple(schema1.name, tup)

for tup in [schema2.pack(schema2.instantiate(i, random.randint(0,50), 'This is a testing tuple.')) for i in range(0,2000)]:
  _ = db.insertTuple(schema2.name, tup)

keySchema  = DBSchema('employeeKey',  [('id', 'int'), ('age', 'int')])
keySchema2 = DBSchema('employeeKey2', [('id2', 'int'), ('age2', 'int')])
  
query5 = db.query().fromTable('employee').join( \
          db.query().fromTable('employee'), \
          rhsSchema=schema2, \
          method='hash', \
          lhsHashFn='hash(id) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(id2) % 4', rhsKeySchema=keySchema2, \
        ).finalize()

print(query5.explain());

def readResult():
    for page in db.processQuery(query5):
        for tup in page[1]:
            yield query5.schema().unpack(tup);

q5result = readResult();

while( q5result ):
    print(next(q5result));