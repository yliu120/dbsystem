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
SQL Query. Question 2:
  select part.name, count(*) as count
   from part, lineitem
   where part.partkey = lineitem.partkey and lineitem.returnflag = 'R'
   group by part.name;
'''
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

# 
keySchema31 = DBSchema('groupByKey', [('L_ORDERKEY', 'int'),('L_PARTKEY', 'int')]);
aggSchema31 = DBSchema('groupByagg', [('num1', 'int')]);

keySchema32 = DBSchema('groupByKey1', [('N_NAME', 'char(25)'),('p_name', 'char(55)')]);
aggSchema32 = DBSchema('groupByagg1', [('num', 'int')]);

query31 = db.query().fromTable('nation').join(\
           db.query().fromTable('customer').select({'c_nationkey': ('C_NATIONKEY', 'int'), 'c_custkey': ('C_CUSTKEY','int')}).join( \
             db.query().fromTable('orders').select({'o_custkey': ('O_CUSTKEY','int'), 'o_orderkey' : ('O_ORDERKEY', 'int')}).join( \
               db.query().fromTable('lineitem').groupBy(
                 groupSchema=keySchema31, \
                 aggSchema=aggSchema31, \
                 groupExpr=(lambda e: (e.L_ORDERKEY, e.L_PARTKEY)), \
                 aggExprs=[(0, lambda acc, e: acc + e.L_QUANTITY, lambda x: x)], \
                 groupHashFn=(lambda gbVal: gbVal % 10)).join( \
                   db.query().fromTable('part').select({'p_name' : ('P_NAME', 'char(55)'), 'p_partkey' : ('P_PARTKEY','int')}), \
                   method='block-nested-loops', expr="p_partkey == L_PARTKEY"), \
                     method='block-nested-loops', expr="o_orderkey = L_ORDERKEY"), \
                       method='block-nested-loops', expr="c_custkey = o_custkey"), \
                         method='block-nested-loops', expr="c_nationkey = N_NATIONKEY").groupBy( \
                           groupSchema=keySchema32, \
                           aggSchema=aggSchema32, \
                           groupExpr=(lambda e: (e.N_NAME, e.p_name)), \
                           aggExprs=[(0, lambda acc, e: acc + e.num1, lambda x: x)], \
                           groupHashFn=(lambda gbVal: hash(gbVal) % 10));
                         
start = time();
for line in readResult(query31):
  print(line);
end = time();
print("Time for query 31: " + str(end-start));
