from Database import Database
from Catalog.Schema import DBSchema

db = Database(dataDir='./data');
'''
groupKeySchema = DBSchema('groupKey', [('ONE', 'int')]);
groupAggSchema = DBSchema('groupBy', [('S_ACCTBAL','float')]);

query1 = db.query().fromTable('supplier').groupBy( \
              groupSchema=groupKeySchema, \
              aggSchema=groupAggSchema, \
              groupExpr=(lambda e: 1), \
              aggExprs=[(0, lambda acc, e: acc + e.S_ACCTBAL, lambda x: x)], \
              groupHashFn=(lambda gbVal: hash(gbVal) % 1)).finalize();


print( query1.explain() );


for page in db.processQuery( query1 ):
  for tup in page[1]:
    print( query1.schema().unpack(tup) );

'''
'''
groupKeySchema = DBSchema('groupKey', [('ONE', 'int')]);
groupAggSchema = DBSchema('groupBy', [('revenue','float')]);

query1 = db.query().fromTable('lineitem').where( \
            "L_SHIPDATE >= 19940101 and L_SHIPDATE < 19950101 and \
            L_DISCOUNT >= 0.06 - 0.01 and L_DISCOUNT <= 0.06 + 0.01 and L_QUANTITY < 24").groupBy( \
            groupSchema=groupKeySchema, \
            aggSchema=groupAggSchema, \
            groupExpr=(lambda e: 1), \
            aggExprs=[(0, lambda acc, e: acc + e.L_EXTENDEDPRICE * e.L_DISCOUNT, lambda x: x)], \
            groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select( \
            {'revenue' : ('revenue', 'float')}).finalize();

# query1.sample(10);

print( query1.explain() );

for page in db.processQuery( query1 ):
  for tup in page[1]:
    print( query1.schema().unpack(tup) );
'''
'''
queryOpt = db.optimizer.pushdownOperators( query1 );

print( queryOpt.explain() );

for page in db.processQuery( queryOpt ):
  for tup in page[1]:
    print( queryOpt.schema().unpack(tup) );
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
print( query2.explain() );

for page in db.processQuery( query2 ):
  for tup in page[1]:
    print( query2.schema().unpack(tup) );