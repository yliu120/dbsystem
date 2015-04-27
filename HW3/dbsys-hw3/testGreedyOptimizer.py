import Database
from Query.Optimizer import Optimizer
from Query.GreedyOptimizer import GreedyOptimizer
from Catalog.Schema import DBSchema
from time import time

db = Database.Database()
deptSchema = DBSchema('department', [('d_id', 'int'), ('d_name', 'char(30)')]);
emplSchema = DBSchema('employee', [('e_id', 'int'), ('e_name', 'char(30)'), ('e_projectid', 'int')])
projSchema = DBSchema('project', [('p_id','int'), ('p_name', 'char(30)')])
gratSchema = DBSchema('grant', [('g_id','int'), ('g_projectid', 'int'), ('g_source', 'char(30)')])
synSchema1 = DBSchema('syn1', [('a','int'), ('b', 'char(30)')])
synSchema2 = DBSchema('syn2', [('c','int'), ('d', 'char(30)'), ('e','int')])

db.createRelation('department', [('d_id', 'int'), ('d_name', 'char(30)')])
db.createRelation('employee', [('e_id', 'int'), ('e_name', 'char(30)'), ('e_projectid', 'int')])
db.createRelation('project', [('p_id','int'), ('p_name', 'char(30)')])
db.createRelation('grant', [('g_id','int'), ('g_projectid', 'int'), ('g_source', 'char(30)')])
db.createRelation('syn1', [('a','int'), ('b', 'char(30)')]);
db.createRelation('syn2', [('c','int'), ('d', 'char(30)'), ('e','int')]);

for tup in [deptSchema.pack(deptSchema.instantiate(i, "Nature"+str(i))) for i in range(4000)]:
  _ = db.insertTuple('department', tup);
for tup in [deptSchema.pack(deptSchema.instantiate(i, "Science"+str(i))) for i in range(4000, 8000)]:
  _ = db.insertTuple('department', tup);
ename = ["John", "Mike", "Davis", "Alex"];
for tup in [emplSchema.pack(emplSchema.instantiate(i, ename[i%4], i%10)) for i in range(8000)]:
  _ = db.insertTuple('employee', tup);
projectName = ["CS","EE","Biophysics","Biostats","NeuroScience", "Cell Biology"];
for tup in [projSchema.pack(projSchema.instantiate(i, projectName[i%6])) for i in range(8000)]:
  _ = db.insertTuple('project', tup);
sourceName = ["NIH","NSF","Apple","Microsoft","Google"];
for tup in [gratSchema.pack(gratSchema.instantiate(i, i%2000, sourceName[i%5])) for i in range(8000)]:
  _ = db.insertTuple('grant', tup);
for tup in [synSchema1.pack(synSchema1.instantiate(i, sourceName[i%3])) for i in range(8000)]:
  _ = db.insertTuple('syn1', tup);
for tup in [synSchema2.pack(synSchema2.instantiate(i, sourceName[i%5], i%500)) for i in range(8000)]:
  _ = db.insertTuple('syn2', tup);

query2 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='e_id == d_id').finalize();
        
query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='e_id == d_id').join( \
        db.query().fromTable('project'), \
        method='block-nested-loops', expr='e_projectid == p_id').join( \
        db.query().fromTable('grant'), \
        method='block-nested-loops', expr='p_id == g_projectid').finalize();

query6 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='e_id == d_id').join( \
        db.query().fromTable('project'), \
        method='block-nested-loops', expr='e_projectid == p_id').join( \
        db.query().fromTable('grant'), \
        method='block-nested-loops', expr='g_projectid == p_id').join( \
        db.query().fromTable('syn1'), \
        method='block-nested-loops', expr='a == g_id').join( \
        db.query().fromTable('syn2'), \
        method='block-nested-loops', expr='a == c').finalize();      
        
db.optimizer = GreedyOptimizer(db);

print("Testing Greedy Optimizer.")
# Testing using synthetic data set.
start = time();
db.optimizer.pickJoinOrder(query2);
end = time();
print("Joining 2 Plans uses:" + str(end - start));

db.optimizer = GreedyOptimizer(db);

start = time();
db.optimizer.pickJoinOrder(query4);
end = time();
print("Joining 4 Plans uses:" + str(end - start));

db.optimizer = GreedyOptimizer(db);

start = time();
db.optimizer.pickJoinOrder(query6);
end = time();
print("Joining 6 Plans uses:" + str(end - start));

db.removeRelation('department');
db.removeRelation('employee');
db.removeRelation('project');
db.removeRelation('grant');
db.removeRelation('syn1');
db.removeRelation('syn2');
db.close();