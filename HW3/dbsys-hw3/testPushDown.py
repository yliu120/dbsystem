from Database import Database
from Catalog.Schema import DBSchema

db = Database(dataDir='./data');

'''
SQL Query. Question 2:
  select part.name, count(*) as count
   from part, lineitem
   where part.partkey = lineitem.partkey and lineitem.returnflag = 'R'
   group by part.name;
'''

ls1 = DBSchema('partkey1',[('P_PARTKEY', 'int')]);
rs1 = DBSchema('partkey2',[('L_PARTKEY', 'int')]);

pSchema = db.relationSchema('part');
lSchema = DBSchema('liselect',[('L_PARTKEY', 'int')]);
keySchema = DBSchema('groupByKey', [('P_NAME', 'char(55)')]);
groupBySchema = DBSchema('groupBy', [('count','int')]);
          
query = db.query().fromTable('part').join( \
          db.query().fromTable('lineitem'), \
          method = 'hash', \
          lhsHashFn = 'hash(P_PARTKEY) % 4', lhsKeySchema = ls1, \
          rhsHashFn = 'hash(L_PARTKEY) % 4', rhsKeySchema = rs1).where("L_RETURNFLAG == 'R' and P_PARTKEY > 10000 and (L_PARTKEY + P_PARTKEY) > 20000").select({'P_NAME': ('P_NAME', 'char(55)'), 'P_PARTKEY': ('P_PARTKEY', 'int')}).groupBy( \
            groupSchema=keySchema, \
            aggSchema=groupBySchema, \
            groupExpr=(lambda e: e.P_NAME), \
            aggExprs=[(0, lambda acc, e: acc + 1, lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 10)).finalize();

print( query.explain() );

queryOpt = db.optimizer.pushdownOperators( query );

print( queryOpt.explain() );

# queryTest = db.query().fromTable('part').select({'P_NAME': ('P_NAME', 'char(55)'), 'P_PARTKEY': ('(P_PARTKEY+1)', 'int')}).finalize();

for page in db.processQuery( queryOpt ):
        for tup in page[1]:
            print( queryOpt.schema().unpack(tup) );

