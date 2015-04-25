from Database import Database
from Catalog.Schema import DBSchema

db = Database(dataDir='./data');

'''
exp 3

TPC-H Query 3: a 3-way join and aggregate query

select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        customer,
        orders,
        lineitem
where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < 19950315
        and l_shipdate > 19950315
group by
        l_orderkey,
        o_orderdate,
        o_shippriority
'''
ls1 = DBSchema('customerKey1', [('C_CUSTKEY', 'int')]);
rs1 = DBSchema('customerKey2', [('O_CUSTKEY', 'int')]);

ls2 = DBSchema('orderKey1', [('O_ORDERKEY', 'int')]);
rs2 = DBSchema('orderkey2', [('L_ORDERKEY', 'int')]);

groupKeySchema = DBSchema('groupKey', [('L_ORDERKEY', 'int'), ('O_ORDERDATE', 'int'), ('O_SHIPPRIORITY', 'int')]);
groupAggSchema = DBSchema('groupAgg', [('revenue','float')]);

query3 = db.query().fromTable('customer').join( \
            db.query().fromTable('orders'),
            method = 'hash', \
            lhsHashFn = 'hash(C_CUSTKEY) % 5', lhsKeySchema = ls1, \
            rhsHashFn = 'hash(O_CUSTKEY) % 5', rhsKeySchema = rs1).join( \
            db.query().fromTable('lineitem'),
            method = 'hash', \
            lhsHashFn = 'hash(O_ORDERKEY) % 5', lhsKeySchema = ls2, \
            rhsHashFn = 'hash(L_ORDERKEY) % 5', rhsKeySchema = rs2).where( \
            "(C_MKTSEGMENT == 'BUILDING') and (O_ORDERDATE < 19950315) and (L_SHIPDATE > 19950315)").groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: (e.L_ORDERKEY, e.O_ORDERDATE, e.O_SHIPPRIORITY)), \
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 10)).select( \
            {'l_orderkey' : ('L_ORDERKEY', 'int'), \
             'revenue' : ('revenue', 'float'), \
             'o_orderdate' : ('O_ORDERDATE', 'int'), \
             'o_shippriority' : ('O_SHIPPRIORITY', 'int')}).finalize();

print( query3.explain() );
             
query3PD = db.optimizer.optimizeQuery( query3 );

print( query3PD.explain() );



  
#for page in db.processQuery( query1 ):
#    for tup in page[1]:
#        print( query1.schema().unpack(tup) );

