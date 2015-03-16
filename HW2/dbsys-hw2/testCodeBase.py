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
'''
#Query 1 -- hash join:

lhsKeySchema1 = DBSchema('part', [('P_PARTKEY','int')])
rhsKeySchema1 = DBSchema('partsupp', [('PS_PARTKEY', 'int')])

lhsKeySchema2 = DBSchema('partsupp', [('PS_SUPPKEY', 'int')])
rhsKeySchema2 = DBSchema('supplier', [('S_SUPPKEY','int')])

query_part = db.query().fromTable('part').\
    select({'P_NAME':('P_NAME','char(55)'), 'P_PARTKEY':('P_PARTKEY','int')})
query_partsupp = db.query().fromTable('partsupp').where('PS_AVAILQTY == 1').\
    select({'PS_PARTKEY':('PS_PARTKEY','int'), 'PS_SUPPKEY':('PS_SUPPKEY','int')})
query_supplier = db.query().fromTable('supplier').\
    select({'S_NAME':('S_NAME','char(25)'), 'S_SUPPKEY':('S_SUPPKEY', 'int')})

query_join_part_partsupp = query_part.join(\
    query_partsupp,\
    rhsSchema = query_partsupp.finalize().schema(),\
    method = 'hash',\
    lhsHashFn = 'hash(P_PARTKEY) % 5', lhsKeySchema = lhsKeySchema1,\
    rhsHashFn = 'hash(PS_PARTKEY) % 5', rhsKeySchema = rhsKeySchema1, \
    )

query_join_part_partsupp_supplier = query_join_part_partsupp.join(\
    query_supplier,\
    rhsSchema = query_supplier.finalize().schema(),\
    method = 'hash',\
    lhsHashFn = 'hash(PS_SUPPKEY) % 5', lhsKeySchema = lhsKeySchema2,\
    rhsHashFn = 'hash(S_SUPPKEY) % 5', rhsKeySchema = rhsKeySchema2,\
    ).select({'P_NAME':('P_NAME','char(55)'), 'S_NAME':('S_NAME','char(25)')})

query_partsupp2 = db.query().fromTable('partsupp').where('PS_SUPPLYCOST < 5').\
    select({'PS_PARTKEY':('PS_PARTKEY','int'), 'PS_SUPPKEY':('PS_SUPPKEY','int')})

query_join_part_partsupp2 = query_part.join(\
    query_partsupp2,\
    rhsSchema = query_partsupp.finalize().schema(),\
    method = 'hash',\
    lhsHashFn = 'hash(P_PARTKEY) % 5', lhsKeySchema = lhsKeySchema1,\
    rhsHashFn = 'hash(PS_PARTKEY) % 5', rhsKeySchema = rhsKeySchema1, \
    )

query_join_part_partsupp_supplier2 = query_join_part_partsupp2.join(\
    query_supplier,\
    rhsSchema = query_supplier.finalize().schema(),\
    method = 'hash',\
    lhsHashFn = 'hash(PS_SUPPKEY) % 5', lhsKeySchema = lhsKeySchema2,\
    rhsHashFn = 'hash(S_SUPPKEY) % 5', rhsKeySchema = rhsKeySchema2,\
    ).select({'P_NAME':('P_NAME','char(55)'), 'S_NAME':('S_NAME','char(25)')})

query_union_all =  query_join_part_partsupp_supplier.union(\
    query_join_part_partsupp_supplier2).finalize()

start = time()
for line in readResult(query_union_all):
  print(line);
end = time()
print("Execution time: " + str(end - start))
'''
'''
SQL Query. Question 2:
  select part.name, count(*) as count
   from part, lineitem
   where part.partkey = lineitem.partkey and lineitem.returnflag = 'R'
   group by part.name;

# Query with our codebase with BNLJ
pSchema = db.relationSchema('part');
lSchema = DBSchema('liselect',[('l_partkey', 'int')]);
keySchema = DBSchema('groupByKey', [('p_name', 'char(55)')]);
groupBySchema = DBSchema('groupBy', [('count','int')]);

query21 = db.query().fromTable('part').select({'p_name': ('P_NAME', 'char(55)'), 'p_partkey': ('P_PARTKEY', 'int')}).join( \
        db.query().fromTable('lineitem').where("L_RETURNFLAG == 'R'").select({'l_partkey': ('L_PARTKEY', 'int')}), \
        rhsSchema=lSchema, \
        method='block-nested-loops', expr="p_partkey == l_partkey").groupBy( \
        groupSchema=keySchema, \
          aggSchema=groupBySchema, \
          groupExpr=(lambda e: e.p_name), \
          aggExprs=[(0, lambda acc, e: acc + 1, lambda x: x)], \
          groupHashFn=(lambda gbVal: gbVal % 10)).finalize();

start = time();
for line in readResult(query21):
  print(line);
end = time();
print("Time for query 21: " + str(end-start));
# Query with our codebase with HashJoin

ls1 = DBSchema('partkey1',[('p_partkey', 'int')]);
rs1 = DBSchema('partkey2',[('l_partkey', 'int')]);

query22 = db.query().fromTable('part').select({'p_name': ('P_NAME', 'char(55)'), 'p_partkey': ('P_PARTKEY', 'int')}).join( \
        db.query().fromTable('lineitem').where("L_RETURNFLAG == 'R'").select({'l_partkey': ('L_PARTKEY', 'int')}), \
        rhsSchema=lSchema, \
        method='hash', \
        lhsHashFn='hash(partkey) % 10',  lhsKeySchema=ls1, \
        rhsHashFn='hash(lkey) % 10', rhsKeySchema=rs1,).groupBy( \
        groupSchema=keySchema, \
          aggSchema=groupBySchema, \
          groupExpr=(lambda e: e.p_name), \
          aggExprs=[(0, lambda acc, e: acc + 1, lambda x: x)], \
          groupHashFn=(lambda gbVal: gbVal % 10)).finalize();
          
start = time();
for line in readResult(query22):
  print(line);
end = time();
print("Time for query 22: " + str(end-start));
'''

# 
'''
ls1 = DBSchema('partkey1',[('p_partkey', 'int')]);
rs1 = DBSchema('partkey2',[('L_PARTKEY', 'int')]);

pSchema = db.relationSchema('part');
lSchema = DBSchema('liselect',[('L_PARTKEY', 'int')]);
keySchema = DBSchema('groupByKey', [('p_name', 'char(55)')]);
groupBySchema = DBSchema('groupBy', [('count','int')]);

query2hash = db.query().fromTable('part').select({'p_name': ('P_NAME', 'char(55)'), 'p_partkey': ('P_PARTKEY', 'int')}).join( \
        db.query().fromTable('lineitem').where("L_RETURNFLAG == 'R'"), \
        method='hash', \
        lhsHashFn='hash(p_partkey) % 10',  lhsKeySchema=ls1, \
        rhsHashFn='hash(L_PARTKEY) % 10', rhsKeySchema=rs1).groupBy( \
        groupSchema=keySchema, \
          aggSchema=groupBySchema, \
          groupExpr=(lambda e: e.p_name), \
          aggExprs=[(0, lambda acc, e: acc + 1, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 10)).finalize();
          
start = time();
for line in readResult(query22):
  print(line);
end = time();
print("Time for query2hash: " + str(end-start));
'''
'''
SQL Query. Question 3:
  create table temp as
   select n.name as nation, p.name as part, sum(l.quantity) as num
   from customer c, nation n, orders o, lineitem l, part p
   where c.nationkey = n.nationkey
     and c.custkey = o.custkey
     and o.orderkey = l.orderkey
     and l.partkey = p.partkey
   group by n.name, p.name;

 select nation, max(num)
  from temp
  group by nation;
  
  Note that lineitem is large. We can groupby lineitem with l.orderkey and l.partkey first to create
  a smaller dataset.
  
  Then nation < part < customer < orders
'''

query3hash = db.query().fromTable('nation').join(\
           db.query().fromTable('customer'), \
             method='hash', \
             lhsHashFn='hash(N_NATIONKEY) % 10', lhsKeySchema=DBSchema('ls1',[('N_NATIONKEY','int')]), \
             rhsHashFn='hash(C_NATIONKEY) % 10', rhsKeySchema=DBSchema('rs1',[('C_NATIONKEY','int')])).join( \
               db.query().fromTable('orders').select({'O_ORDERKEY':('O_ORDERKEY','int'), 'O_CUSTKEY': ('O_CUSTKEY', 'int')}), \
                 method='hash', \
                 lhsHashFn='hash(C_CUSTKEY) % 10', lhsKeySchema=DBSchema('ls2',[('C_CUSTKEY','int')]), \
                 rhsHashFn='hash(O_CUSTKEY) % 10', rhsKeySchema=DBSchema('rs2',[('O_CUSTKEY','int')])).join( \
                   db.query().fromTable('lineitem').select({'L_ORDERKEY' : ('L_ORDERKEY', 'int'), 'L_PARTKEY':('L_PARTKEY', 'int'), 'L_QUANTITY': ('L_QUANTITY', 'float')}), \
                     method='hash', \
                     lhsHashFn='hash(O_ORDERKEY) % 10', lhsKeySchema=DBSchema('ls3',[('O_ORDERKEY','int')]), \
                     rhsHashFn='hash(L_ORDERKEY) % 10', rhsKeySchema=DBSchema('rs3',[('L_ORDERKEY','int')])).join( \
                       db.query().fromTable('part').select({'p_name': ('P_NAME', 'char(55)'), 'p_partkey': ('P_PARTKEY', 'int')}), \
                       method='hash', \
                       lhsHashFn='hash(L_PARTKEY) % 10', lhsKeySchema=DBSchema('ls4',[('L_PARTKEY','int')]), \
                       rhsHashFn='hash(p_partkey) % 10', rhsKeySchema=DBSchema('rs4',[('p_partkey','int')])).groupBy( \
                        groupSchema=DBSchema('gb1',[('N_NAME','char(25)'), ('p_name','char(55)')]), \
                        aggSchema=DBSchema('agg1',[('num','float')]), \
                        groupExpr=(lambda e: (e.N_NAME, e.p_name) ), \
                        aggExprs=[(0, lambda acc, e: acc + e.L_QUANTITY, lambda x: x)], \
                        groupHashFn=(lambda gbVal: hash(gbVal) % 20)
                     ).groupBy( \
                         groupSchema=DBSchema('gb2',[('N_NAME','char(25)')]), \
                         aggSchema=DBSchema('agg1',[('max','float')]), \
                         groupExpr=(lambda e: e.N_NAME ), \
                         aggExprs=[(0, lambda acc, e: max(acc, e.num), lambda x: x)], \
                         groupHashFn=(lambda gbVal: hash(gbVal) % 10) ).finalize();

print(query3hash.explain());

start = time();
for line in readResult(query3hash):
  print(line);
end = time();
print("Time for query3hash: " + str(end-start));           
                 
                 