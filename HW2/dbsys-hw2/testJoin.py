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
db.createRelation('employee', [('id', 'int'), ('age', 'int'), ('description', 'char(40)')])
schema = db.relationSchema('employee')

for tup in [schema.pack(schema.instantiate(i, random.randint(0,50), 'This is a testing tuple.')) for i in range(0,10000)]:
  _ = db.insertTuple(schema.name, tup)

 ### SELECT * FROM Employee E1 JOIN Employee E2 ON E1.id = E2.id
e2schema = schema.rename('employee2', {'id':'id2', 'age':'age2'}) 

query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('employee'), \
        rhsSchema=e2schema, \
        method='block-nested-loops', expr='id == id2').finalize()

print(query4.explain())

q4results = [query4.schema().unpack(tup) for page in db.processQuery(query4) for tup in page[1]]


print([(tup.id, tup.id2) for tup in q4results]);
print ([tup for tup in q4results]);
print( len(q4results) );
