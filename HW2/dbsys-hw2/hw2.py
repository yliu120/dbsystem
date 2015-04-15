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

lhsKeySchema1 = DBSchema('partsupp', [('PS_PARTKEY', 'int')])
rhsKeySchema1 = DBSchema('part', [('P_PARTKEY','int')])

lhsKeySchema2 = DBSchema('partsupp', [('PS_SUPPKEY', 'int')])
rhsKeySchema2 = DBSchema('supplier', [('S_SUPPKEY','int')])

part = db.query().fromTable('part').select({'P_NAME':('P_NAME','char(55)'), 'P_PARTKEY':('P_PARTKEY','int')});
partsupp = db.query().fromTable('partsupp').where('PS_AVAILQTY == 1').select({'PS_PARTKEY':('PS_PARTKEY','int'), 'PS_SUPPKEY':('PS_SUPPKEY','int')})
supplier = db.query().fromTable('supplier').select({'S_NAME':('S_NAME','char(25)'), 'S_SUPPKEY':('S_SUPPKEY', 'int')})

join_ps_p = partsupp.join(\
              part, \
              rhsSchema = DBSchema('part', [('P_NAME','char(55)'), ('P_PARTKEY','int')]), \
              method = 'hash', \
              lhsHashFn = lambda e: e.PS_PARTKEY % 5, lhsKeySchema = lhsKeySchema1,\
              rhsHashFn = lambda e: e.P_PARTKEY % 5, rhsKeySchema = rhsKeySchema1);
              
join_three = join_ps_p.join(\
               supplier, \
               rhsSchema = DBSchema('supplier', [('S_NAME','char(25)'), ('S_SUPPKEY', 'int')]), \
               method = 'hash',
               lhsHashFn = lambda e: e.PS_SUPPKEY % 5, lhsKeySchema = lhsKeySchema2,\
               rhsHashFn = lambda e: e.S_SUPPKEY % 5, rhsKeySchema = rhsKeySchema2,\
               ).select({'P_NAME':('P_NAME','char(55)'), 'S_NAME':('S_NAME','char(25)')});

partsupp2 = db.query().fromTable('partsupp').where('PS_SUPPLYCOST < 5').select({'PS_PARTKEY':('PS_PARTKEY','int'), 'PS_SUPPKEY':('PS_SUPPKEY','int')})

join_ps_p2 = partsupp2.join(\
              part, \
              rhsSchema = DBSchema('part', [('P_NAME','char(55)'), ('P_PARTKEY','int')]), \
              method = 'hash', \
              lhsHashFn = lambda e: e.PS_PARTKEY % 5, lhsKeySchema = lhsKeySchema1,\
              rhsHashFn = lambda e: e.P_PARTKEY % 5, rhsKeySchema = rhsKeySchema1);
              
join_three2 = join_ps_p2.join(\
               supplier, \
               rhsSchema = DBSchema('supplier', [('S_NAME','char(25)'), ('S_SUPPKEY', 'int')]), \
               method = 'hash',
               lhsHashFn = lambda e: e.PS_SUPPKEY % 5, lhsKeySchema = lhsKeySchema2,\
               rhsHashFn = lambda e: e.S_SUPPKEY % 5, rhsKeySchema = rhsKeySchema2,\
               ).select({'P_NAME':('P_NAME','char(55)'), 'S_NAME':('S_NAME','char(25)')});
               
query1hash = join_three.union( join_three2 ).finalize();
print(query1hash.explain())


start = time()
for line in readResult(query1hash):
  print(line);
end = time()
print("Execution time: " + str(end - start))

'''
SQL Query. Question 2:
  select part.name, count(*) as count
   from part, lineitem
   where part.partkey = lineitem.partkey and lineitem.returnflag = 'R'
   group by part.name;


'''

# 
ls1 = DBSchema('partkey1',[('p_partkey', 'int')]);
rs1 = DBSchema('partkey2',[('L_PARTKEY', 'int')]);

pSchema = db.relationSchema('part');
lSchema = DBSchema('liselect',[('L_PARTKEY', 'int')]);
keySchema = DBSchema('groupByKey', [('p_name', 'char(55)')]);
groupBySchema = DBSchema('groupBy', [('count','int')]);

query2hash = db.query().fromTable('part').select({'p_name': ('P_NAME', 'char(55)'), 'p_partkey': ('P_PARTKEY', 'int')}).join( \
        db.query().fromTable('lineitem').where("L_RETURNFLAG == 'R'"), \
        method='hash', \
        lhsHashFn = lambda e: e.p_partkey % 10,  lhsKeySchema=ls1, \
        rhsHashFn = lambda e: e.L_PARTKEY % 10, rhsKeySchema=rs1).groupBy( \
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

# prepare queries
nation = db.query().fromTable('nation').select({'N_NATIONKEY':('N_NATIONKEY','int'), 'N_NAME':('N_NAME', 'char(25)')});
part   = db.query().fromTable('part').select({'P_PARTKEY':('P_PARTKEY','int'), 'P_NAME':('P_NAME','char(55)')});
orders = db.query().fromTable('orders').select({'O_ORDERKEY':('O_ORDERKEY','int'), 'O_CUSTKEY':('O_CUSTKEY','int')});
line   = db.query().fromTable('lineitem').select({'L_ORDERKEY':('L_ORDERKEY','int'),'L_PARTKEY':('L_PARTKEY','int'), 'L_QUANTITY':('L_QUANTITY','float')});
customer = db.query().fromTable('customer').select({'C_NATIONKEY':('C_NATIONKEY','int'),'C_CUSTKEY':('C_CUSTKEY','int')});

nc = nation.join(\
       customer, \
       rhsSchema = DBSchema('c',[('C_NATIONKEY','int'),('C_CUSTKEY','int')]), \
       method = 'hash', \
       lhsHashFn=lambda e : e.N_NATIONKEY % 5, lhsKeySchema=DBSchema('ls1',[('N_NATIONKEY','int')]), \
       rhsHashFn=lambda e : e.C_NATIONKEY % 5, rhsKeySchema=DBSchema('rs1',[('C_NATIONKEY','int')]))

nco = nc.join(\
       orders, \
       method = 'hash', \
       lhsHashFn=lambda e : e.C_CUSTKEY % 5, lhsKeySchema=DBSchema('ls2',[('C_CUSTKEY','int')]), \
       rhsHashFn=lambda e : e.O_CUSTKEY % 5, rhsKeySchema=DBSchema('rs2',[('O_CUSTKEY','int')]))

ncol = nco.join(\
        line, \
        rhsSchema = DBSchema('l',[('L_ORDERKEY','int'),('L_PARTKEY','int'),('L_QUANTITY','float')]), \
        method = 'hash', \
        lhsHashFn=lambda e : e.O_ORDERKEY % 5, lhsKeySchema=DBSchema('ls3',[('O_ORDERKEY','int')]), \
        rhsHashFn=lambda e : e.L_ORDERKEY % 5, rhsKeySchema=DBSchema('rs3',[('L_ORDERKEY','int')]))

all  = ncol.join(\
        part, \
        rhsSchema = DBSchema('p', [('P_PARTKEY','int'),('P_NAME','char(55)')]),\
        method = 'hash', \
        lhsHashFn=lambda e : e.L_PARTKEY % 5, lhsKeySchema=DBSchema('ls4',[('L_PARTKEY','int')]), \
        rhsHashFn=lambda e : e.P_PARTKEY % 5, rhsKeySchema=DBSchema('rs4',[('P_PARTKEY','int')])
        )

allgroup1 = all.groupBy(\
              groupSchema=DBSchema('gb1',[('N_NAME','char(25)'), ('P_NAME','char(55)')]), \
              aggSchema=DBSchema('agg1',[('num','float')]), \
              groupExpr=(lambda e: (e.N_NAME, e.P_NAME) ), \
              aggExprs=[(0, lambda acc, e: acc + e.L_QUANTITY, lambda x: x)], \
              groupHashFn=(lambda gbVal: hash(gbVal) % 10)
             )

query3hash = allgroup1.groupBy(\
              groupSchema=DBSchema('gb2',[('N_NAME','char(25)')]), \
              aggSchema=DBSchema('agg1',[('max','float')]), \
              groupExpr=(lambda e: e.N_NAME ), \
              aggExprs=[(0, lambda acc, e: max(acc, e.num), lambda x: x)], \
              groupHashFn=(lambda gbVal: hash(gbVal) % 10) ).finalize();


start = time();
for line in readResult(query3hash):
  print(line);
end = time();
print("Time for query3hash: " + str(end-start));           
                 
                 