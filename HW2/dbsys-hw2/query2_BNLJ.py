from Database import Database
from Catalog.Schema import DBSchema
from time import time

db = Database(dataDir='./data')

def readResult( query ):
    for page in db.processQuery(query):
        for tup in page[1]:
            yield query.schema().unpack(tup);
            
'''
SQL Query. Question 2:
  select part.name, count(*) as count
   from part, lineitem
   where part.partkey = lineitem.partkey and lineitem.returnflag = 'R'
   group by part.name;


'''

pSchema = db.relationSchema('part');
lSchema = DBSchema('liselect',[('L_PARTKEY', 'int')]);
keySchema = DBSchema('groupByKey', [('p_name', 'char(55)')]);
groupBySchema = DBSchema('groupBy', [('count','int')]);

query2BNLJ = db.query().fromTable('part').select({'p_name': ('P_NAME', 'char(55)'), 'p_partkey': ('P_PARTKEY', 'int')}).join( \
        db.query().fromTable('lineitem').where("L_RETURNFLAG == 'R'"), \
        method='block-nested-loops', \
        expr = 'p_partkey == L_PARTKEY').groupBy( \
        groupSchema=keySchema, \
          aggSchema=groupBySchema, \
          groupExpr=(lambda e: e.p_name), \
          aggExprs=[(0, lambda acc, e: acc + 1, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 10)).finalize();
          
start = time();
for line in readResult(query2BNLJ):
  print(line);
end = time();
print("Time for query2BNLJ: " + str(end-start));
