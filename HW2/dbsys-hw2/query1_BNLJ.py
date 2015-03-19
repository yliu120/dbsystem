from Database import Database
from Catalog.Schema import DBSchema
from time import time

db = Database(dataDir='./data')

def readResult( query ):
    for page in db.processQuery(query):
        for tup in page[1]:
            yield query.schema().unpack(tup);
            
'''
SQL Query. Question 1:
  select p.name, s.name
   from part p, supplier s, partsupp ps
   where p.partkey = ps.partkey
         and ps.suppkey = s.suppkey
         and ps.availqty = 1
  union all
  select p.name, s.name
   from part p, supplier s, partsupp ps
   where p.partkey = ps.partkey
         and ps.suppkey = s.suppkey
         and ps.supplycost < 5;
'''

#Query 1 -- hash join:


part = db.query().fromTable('part').select({'P_NAME':('P_NAME','char(55)'), 'P_PARTKEY':('P_PARTKEY','int')});
partsupp = db.query().fromTable('partsupp').where('PS_AVAILQTY == 1').select({'PS_PARTKEY':('PS_PARTKEY','int'), 'PS_SUPPKEY':('PS_SUPPKEY','int')})
supplier = db.query().fromTable('supplier').select({'S_NAME':('S_NAME','char(25)'), 'S_SUPPKEY':('S_SUPPKEY', 'int')})

join_ps_p = partsupp.join(\
              part, \
              rhsSchema = DBSchema('part', [('P_NAME','char(55)'), ('P_PARTKEY','int')]), \
              method = 'block-nested-loops', \
              expr = 'PS_PARTKEY == P_PARTKEY');
              
join_three = join_ps_p.join(\
               supplier, \
               rhsSchema = DBSchema('supplier', [('S_NAME','char(25)'), ('S_SUPPKEY', 'int')]), \
               method = 'block-nested-loops',
               expr = 'PS_SUPPKEY == S_SUPPKEY'
               ).select({'P_NAME':('P_NAME','char(55)'), 'S_NAME':('S_NAME','char(25)')});

partsupp2 = db.query().fromTable('partsupp').where('PS_SUPPLYCOST < 5').select({'PS_PARTKEY':('PS_PARTKEY','int'), 'PS_SUPPKEY':('PS_SUPPKEY','int')})

join_ps_p2 = partsupp2.join(\
              part, \
              rhsSchema = DBSchema('part', [('P_NAME','char(55)'), ('P_PARTKEY','int')]), \
              method = 'block-nested-loops', \
              expr = 'PS_PARTKEY == P_PARTKEY');
              
join_three2 = join_ps_p2.join(\
               supplier, \
               rhsSchema = DBSchema('supplier', [('S_NAME','char(25)'), ('S_SUPPKEY', 'int')]), \
               method = 'block-nested-loops',
               expr = 'PS_SUPPKEY == S_SUPPKEY'
               ).select({'P_NAME':('P_NAME','char(55)'), 'S_NAME':('S_NAME','char(25)')});
               
query1hash = join_three.union( join_three2 ).finalize();
print(query1hash.explain())


start = time()
for line in readResult(query1hash):
  print(line);
end = time()
print("Execution time: " + str(end - start))