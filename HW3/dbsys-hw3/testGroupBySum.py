from Database import Database
from Catalog.Schema import DBSchema

db = Database(dataDir='./data');
'''
groupKeySchema = DBSchema('groupKey', [('S_ACCTBAL', 'float')]);
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
              groupHashFn=(lambda gbVal: hash(gbVal) % 1)).select( \
              {'revenue' : ('revenue', 'float')}).finalize();

print( query1.explain() );

for page in db.processQuery( query1 ):
  for tup in page[1]:
    print( query1.schema().unpack(tup) );
'''
queryOpt = db.optimizer.pushdownOperators( query1 );

print( queryOpt.explain() );

for page in db.processQuery( queryOpt ):
  for tup in page[1]:
    print( queryOpt.schema().unpack(tup) );
    '''