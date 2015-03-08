import Database
import sys
from collections import deque

from Catalog.Schema  import DBSchema

from Query.Operators.TableScan import TableScan
from Query.Operators.Select    import Select
from Query.Operators.Project   import Project
from Query.Operators.Sort      import Sort
import random


db = Database.Database()
db.createRelation('employee', [('id', 'int'), ('age', 'int')])
schema = db.relationSchema('employee')

for tup in [schema.pack(schema.instantiate(i, random.randint(0,50))) for i in range(0,20000)]:
  _ = db.insertTuple(schema.name, tup)


'''
query7 = db.query().fromTable('employee') \
        .order(sortKeyFn=lambda x: x.age, sortKeyDesc='age') \
        .select({'id': ('id', 'int')}).finalize()
        
print(query6.explain())
q6results = [query6.schema().unpack(tup) for page in db.processQuery(query6) for tup in page[1]]

print(q6results);
print( len(q6results) );
'''
q7 = db.query().fromTable('employee').order(sortKeyFn=lambda x: x.age, sortKeyDesc='age').finalize();
print(q7.explain());

q7results = [q7.schema().unpack(tup) for page in db.processQuery(q7) for tup in page[1]]
print(q7results);