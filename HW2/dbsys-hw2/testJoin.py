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

for tup in [schema1.pack(schema1.instantiate(i, random.randint(0,50), 'This is a testing tuple.')) for i in range(0,1000)]:
  _ = db.insertTuple(schema1.name, tup)

for tup in [schema2.pack(schema2.instantiate(i, random.randint(0,50), 'This is a testing tuple.')) for i in range(0,10000)]:
  _ = db.insertTuple(schema2.name, tup)
 ### SELECT * FROM Employee E1 JOIN Employee E2 ON E1.id = E2.id
 
query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('employee2'), \
        rhsSchema=schema2, \
        method='block-nested-loops', expr='age == age2').finalize()

print(query4.explain())

def readResult():
    for page in db.processQuery(query4):
        for tup in page[1]:
            yield query4.schema().unpack(tup);

q4result = readResult();

while( q4result ):
    print(next(q4result));
