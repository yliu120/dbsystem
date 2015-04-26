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

groupKeySchema = DBSchema('groupKey', [('ONE', 'int')]);
groupAggSchema = DBSchema('groupBy', [('revenue','float')]);

query1 = db.query().fromTable('lineitem').where( \
            "(L_SHIPDATE >= 19940101) and (L_SHIPDATE < 19950101) and \
            (0.06 - 0.01 <= L_DISCOUNT <= 0.06 + 0.01) and (L_QUANTITY < 24)").groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: 1), \
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * e.L_DISCOUNT), lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select( \
            {'revenue' : ('revenue', 'float')}).finalize();

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

ls1 = DBSchema('partkey2',[('L_PARTKEY', 'int')]);
rs1 = DBSchema('partkey1',[('P_PARTKEY', 'int')]);

groupKeySchema = DBSchema('groupKey', [('ONE', 'int')]);
groupAggSchema = DBSchema('groupBy', [('promo_revenue','float')]);

query2 = db.query().fromTable('lineitem').join( \
            db.query().fromTable('part'),
            method = 'hash', \
            lhsHashFn = 'hash(L_PARTKEY) % 5', lhsKeySchema = ls1, \
            rhsHashFn = 'hash(P_PARTKEY) % 5', rhsKeySchema = rs1).where( \
            "L_SHIPDATE >= 19950901 and L_SHIPDATE < 19951001").groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: 1), \
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select( \
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
            db.query().fromTable('orders'),
            method = 'hash', \
            lhsHashFn = 'hash(C_CUSTKEY) % 5', lhsKeySchema = ls1, \
            rhsHashFn = 'hash(O_CUSTKEY) % 5', rhsKeySchema = rs1).join( \
            db.query().fromTable('lineitem'),
            method = 'hash', \
            lhsHashFn = 'hash(O_ORDERKEY) % 5', lhsKeySchema = ls2, \
            rhsHashFn = 'hash(L_ORDERKEY) % 5', rhsKeySchema = rs2).where( \
            "C_MKTSEGMENT == 'BUILDING' and O_ORDERDATE < 19950315 and L_SHIPDATE > 19950315").groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: (e.L_ORDERKEY, e.O_ORDERDATE, e.O_SHIPPRIORITY)), \
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 10)).select( \
            {'l_orderkey' : ('L_ORDERKEY', 'int'), \
             'revenue' : ('revenue', 'float'), \
             'o_orderdate' : ('O_ORDERDATE', 'int'), \
             'o_shippriority' : ('O_SHIPPRIORITY', 'int')}).finalize();
             
'''
query 4
TPC-H Query 10: a 4-way join and group-by aggregate query

select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
from
        customer,
        orders,
        lineitem,
        nation
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= 19931001
        and o_orderdate < 19940101
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
'''

ls1 = DBSchema('customerKey1', [('C_CUSTKEY', 'int')]);
rs1 = DBSchema('customerKey2', [('O_CUSTKEY', 'int')]);

ls2 = DBSchema('orderKey1', [('O_ORDERKEY', 'int')]);
rs2 = DBSchema('orderkey2', [('L_ORDERKEY', 'int')]);

ls3 = DBSchema('nationKey1', [('C_NATIONKEY', 'int')]);
rs3 = DBSchema('nationKey2', [('N_NATIONKEY', 'int')]);

groupKeySchema = DBSchema('groupKey', [('C_CUSTKEY', 'int'), ('C_NAME', 'char(25)'), ('C_ACCTBAL', 'float'), \
                                       ('C_PHONE', 'char(15)'), ('N_NAME', 'char(25)'), ('C_ADDRESS', 'char(40)'), \
                                       ('C_COMMENT', 'char(117)')]);
groupAggSchema = DBSchema('groupAgg', [('revenue','float')]);

query4 = db.query().fromTable('customer').join( \
            db.query().fromTalbe('orders'),
            method = 'hash', \
            lhsHashFn = 'hash(C_CUSTKEY) % 5', lhsKeySchema = ls1, \
            rhsHashFn = 'hash(O_CUSTKEY) % 5', rhsKeySchema = rs1).join( \
            db.query().fromTable('lineitem'),
            method = 'hash', \
            lhsHashFn = 'hash(O_ORDERKEY) % 5', lhsKeySchema = ls2, \
            rhsHashFn = 'hash(L_ORDERKEY) % 5', rhsKeySchema = rs2).join( \
            db.query().fromTable('nation'),
            method = 'hash', \
            lhsHashFn = 'hash(C_NATIONKEY) % 5', lhsKeySchema = ls3, \
            rhsHashFn = 'hash(N_NATIONKEY) % 5', rhsKeySchema = rs3).where( \
            "L_RETURNFLAG == 'R' and O_ORDERDATE < 19940101 and O_ORDERDATE >= 19931001").groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: (e.C_CUSTKEY, e.C_NAME, e.C_ACCTBAL, e.C_PHONE, e.N_NAME, e.C_ADDRESS, e.C_COMMENT)), \
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 10)).select( \
            {'c_custkey' : ('C_CUSTKEY', 'int'), \
             'c_name' : ('C_NAME', 'char(25)'), \
             'revenue' : ('revenue', 'float'), \
             'c_acctbal' : ('C_ACCTBAL', 'float'), \
             'n_name' : ('N_NAME', 'char(25)'), \
             'c_address' : ('C_ADDRESS', 'char(40)'), \
             'c_phone' : ('C_PHONE', 'char(15)'), \
             'c_comment' : ('C_COMMENT', 'char(117)')}).finalize();

'''
query 5

TPC-H Query 5: a 6-way join and aggregate query

select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= 19940101
        and o_orderdate < 19950101
group by
        n_name
'''

ls1 = DBSchema('customerKey1', [('C_CUSTKEY', 'int')]);
rs1 = DBSchema('customerKey2', [('O_CUSTKEY', 'int')]);

ls2 = DBSchema('orderKey1', [('O_ORDERKEY', 'int')]);
rs2 = DBSchema('orderkey2', [('L_ORDERKEY', 'int')]);

ls3 = DBSchema('suppKey1', [('L_SUPPKEY', 'int')]);
rs3 = DBSchema('suppkey2', [('S_SUPPKEY', 'int')]);

ls4 = DBSchema('nationKey1', [('S_NATIONKEY', 'int')]);
rs4 = DBSchema('nationKey2', [('N_NATIONKEY', 'int')]);

ls5 = DBSchema('regionKey1', [('N_REGIONKEY', 'int')]);
rs5 = DBSchema('regionKey2', [('R_REGIONKEY', 'int')]);

groupKeySchema = DBSchema('groupKey', [('N_NAME', 'char(25)')]);
groupAggSchema = DBSchema('groupAgg', [('revenue','float')]);

query5 = db.query().fromTable('customer').join( \
            db.query().fromTalbe('orders'),
            method = 'hash', \
            lhsHashFn = 'hash(C_CUSTKEY) % 5', lhsKeySchema = ls1, \
            rhsHashFn = 'hash(O_CUSTKEY) % 5', rhsKeySchema = rs1).join( \
            db.query().fromTable('lineitem'),
            method = 'hash', \
            lhsHashFn = 'hash(O_ORDERKEY) % 5', lhsKeySchema = ls2, \
            rhsHashFn = 'hash(L_ORDERKEY) % 5', rhsKeySchema = rs2).join( \
            db.query().fromTable('supplier'),
            method = 'hash', \
            lhsHashFn = 'hash(L_SUPPKEY) % 5', lhsKeySchema = ls3, \
            rhsHashFn = 'hash(S_SUPPKEY) % 5', rhsKeySchema = rs3). join( \
            db.query().fromTable('nation'),
            method = 'hash', \
            lhsHashFn = 'hash(S_NATIONKEY) % 5', lhsKeySchema = ls4, \
            rhsHashFn = 'hash(N_NATIONKEY) % 5', rhsKeySchema = rs4).join( \
            db.query().fromTable('region'),
            method = 'hash', \
            lhsHashFn = 'hash(N_REGIONKEY) % 5', lhsKeySchema = ls5, \
            rhsHashFn = 'hash(R_REGIONKEY) % 5', rhsKeySchema = rs5).where( \
            "R_NAME == 'ASIA' and O_ORDERDATE >= 19940101 and O_ORDERDATE < 19950101").groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: e.N_NAME), \
            aggExprs=[(0, lambda acc, e: acc + (e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT)), lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 10)).select( \
            {'n_name' : ('N_NAME', 'char(25)'), \
             'revenue' : ('revenue', 'float')}).finalize();
             
             
db.close();