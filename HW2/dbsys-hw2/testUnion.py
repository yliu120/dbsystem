import Database
import sys
from collections import deque

from Catalog.Schema  import DBSchema

from Query.Operators.TableScan import TableScan
from Query.Operators.Select    import Select
from Query.Operators.Project   import Project
from Query.Operators.GroupBy   import GroupBy
from Query.Operators.Union   import Union



db = Database.Database()
db.createRelation('employee', [('id', 'int'), ('age', 'int')])
schema = db.relationSchema('employee')

for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(0,20000)]:
  _ = db.insertTuple(schema.name, tup)

### SELECT * FROM Employee UNION ALL Employee
query3 = db.query().fromTable('employee').union(db.query().fromTable('employee')).finalize()

print(query3.explain())

q3results = [query3.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1]]

print(q3results);
print( len(q3results) );
