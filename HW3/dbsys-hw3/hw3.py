from Database import Database
from Catalog.Schema import DBSchema

db = Database(dataDir='./data');

'''
exp 1

TPC-H Query 6: a 4-chain filter and aggregate query

select
        sum(l_extendedprice * l_discount) as revenue
from
        lineitem
where
        l_shipdate >= 19940101
        and l_shipdate < 19950101
        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        and l_quantity < 24
'''

groupKeySchema = DBSchema('groupKey', [('revenue_p', 'float')]);
groupAggSchema = DBSchema('groupBy', [('revenue','float')]);

query1 = db.query().fromTable('lineitem').where( \
            "L_SHIPDATE >= 19940101 and L_SHIPDATE < 19950101 and \
            0.06 - 0.01 < L_DISCOUNT < 0.06 + 0.01 and L_QUANTITY < 24"). select( \
            {'revenue_p' : ('L_EXTENDEDPRICE * L_DISCOUNT', 'float')}).groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: 1), \
            aggExprs=[(0, lambda acc, e: acc + e.revenue_p, lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 1)).finalize();

'''
exp 2

TPC-H Query 14: a 2-way join and aggregate query

select
        sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
        lineitem,
        part
where
        l_partkey = p_partkey
        and l_shipdate >= 19950901
        and l_shipdate < 19951001
'''
rs1 = DBSchema('partkey1',[('P_PARTKEY', 'int')]);
ls1 = DBSchema('partkey2',[('L_PARTKEY', 'int')]);

groupKeySchema = DBSchema('groupKey', [('promo_revenue_p', 'float')]);
groupAggSchema = DBSchema('groupBy', [('promo_revenue','float')]);

query2 = db.query().fromTable('lineitem').join( \
            db.query().fromTable('part'),
            method = 'hash', \
            lhsHashFn = 'hash(L_PARTKEY) % 4', lhsKeySchema = ls1, \
            rhsHashFn = 'hash(P_PARTKEY) % 4', rhsKeySchema = rs1).where( \
            "L_SHIPDATE >= 19950901 and L_SHIPDATE < 19951001"). select( \
            {'promo_revenue_p' : ('L_EXTENDEDPRICE * (1 - L_DISCOUNT)', 'float')}).groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: 1), \
            aggExprs=[(0, lambda acc, e: acc + e.promo_revenue_p, lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 1)).selcet( \
            {'promo_revenue' : ('promo_revenue','float')}).finalize();

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
            db.query().fromTalbe('orders'),
            method = 'hash', \
            lhsHashFn = 'hash(C_CUSTKEY) % 4', lhsKeySchema = ls1, \
            rhsHashFn = 'hash(O_CUSTKEY) % 4', rhsKeySchema = rs1).join( \
            db.query().fromTable('lineitem'),
            method = 'hash', \
            lhsHashFn = 'hash(O_ORDERKEY) % 4', lhsKeySchema = ls2, \
            rhsHashFn = 'hash(L_ORDERKEY) % 4', rhsKeySchema = rs2).where( \
            "C_MKTSEGMENT = 'BUILDING' and O_ORDERDATE < 19950315 and L_SHIPDATE > 19950315").groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: (e.L_ORDERKEY, e.O_ORDERDATE, e.O_SHIPPRIORITY)), \
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 10)).select( \
            {'l_orderkey' : ('l_orderkey', 'int'), \
             'revenue' : ('revenue', 'float'), \
             'o_orderdate' : ('o_orderdate', 'int'), \
             'o_shippriority' : ('o_shippriority', 'int')}).finalize();
