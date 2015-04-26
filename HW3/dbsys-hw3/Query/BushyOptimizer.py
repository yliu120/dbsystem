from Query.Optimizer import Optimizer
from itertools import combinations as comb
from itertools import chain
from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo
from Catalog.Schema import DBSchema

# This optimizer consider all bushy trees
class BushyOptimizer(Optimizer):
  
  """
  A query optimization class which implements the dynamic programming optimizer
  algorithm in the slides. In this algorithm, we consider any form of the join
  plan, such as left-deep plan, bushy-plan and right deep plan. 
  
  For performance consideration, we do not provide any real dataset tests for
  this implementation. By the following doctests, we provide an artificial test
  for this optimizer.

  >>> import Database
  >>> from Query.Optimizer import Optimizer
  >>> from Catalog.Schema import DBSchema
  >>> from Query.Plan import Plan
  >>> db = Database.Database()
  >>> deptSchema = DBSchema('department', [('d_id', 'int'), ('d_name', 'char(30)')]);
  >>> emplSchema = DBSchema('employee', [('e_id', 'int'), ('e_name', 'char(30)'), ('e_projectid', 'int')])
  >>> projSchema = DBSchema('project', [('p_id','int'), ('p_name', 'char(50)')])
  >>> gratSchema = DBSchema('grant', [('g_id','int'), ('g_projectid', 'int'), ('g_source', 'char(50)')])
  >>> try:
  ...   db.createRelation('department', [('d_id', 'int'), ('d_name', 'char(30)')])
  ...   db.createRelation('employee', [('e_id', 'int'), ('e_name', 'char(30)'), ('e_projectid', 'int')])
  ...   db.createRelation('project', [('p_id','int'), ('p_name', 'char(50)')])
  ...   db.createRelation('grant', [('g_id','int'), ('g_projectid', 'int'), ('g_source', 'char(50)')])
  ... except ValueError:
  ...   pass
  >>> for tup in [deptSchema.pack(deptSchema.instantiate(i, "Nature"+str(i))) for i in range(10)]:
  ...    _ = db.insertTuple('department', tup);
  >>> for tup in [deptSchema.pack(deptSchema.instantiate(i, "Science"+str(i))) for i in range(10, 20)]:
  ...    _ = db.insertTuple('department', tup);
  >>> ename = ["John", "Mike", "Davis", "Alex"];
  >>> for tup in [emplSchema.pack(emplSchema.instantiate(i, ename[i%4], i%10)) for i in range(1000)]:
  ...    _ = db.insertTuple('employee', tup);
  >>> projectName = ["CS","EE","Biophysics","Biostats","NeuroScience", "Cell Biology"];
  >>> for tup in [projSchema.pack(projSchema.instantiate(i, projectName[i%6])) for i in range(2000)]:
  ...    _ = db.insertTuple('project', tup);
  >>> sourceName = ["NIH","NSF","Apple","Microsoft","Google"];
  >>> for tup in [gratSchema.pack(gratSchema.instantiate(i, i%2000, sourceName[i%5])) for i in range(4000)]:
  ...    _ = db.insertTuple('grant', tup);
  >>> query = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='e_id == d_id').join( \
        db.query().fromTable('project'), \
        method='block-nested-loops', expr='e_projectid == p_id').join( \
        db.query().fromTable('grant'), \
        method='block-nested-loops', expr='p_id == g_projectid').finalize();

  >>> db.optimizer = BushyOptimizer(db);
  >>> db.optimizer.pickJoinOrder(query);
  >>> db.removeRelation('department');
  >>> db.removeRelation('employee');
  >>> db.removeRelation('project');
  >>> db.removeRelation('grant');
  >>> db.close();
  
  """
  def __init__(self, db):
    super().__init__(db);
    
  # Helper function to tell whether a plan is right-deep-plan
  def isRightDeep(self, operator, aPaths):
    if operator and operator.operatorType()[-4:] == "Join":
      if operator.lhsPlan in aPaths and operator.rhsPlan in aPaths:
        return True;
      elif operator.lhsPlan in aPaths and operator.rhsPlan.operatorType()[-4:] == "Join":
        return self.isRightDeep(operator.rhsPlan, aPaths);
      else:
        return False;
    else:
      raise ValueError("Internally we don't stress this function to other cases");

  # Helper function to test whether two plans are joinable
  def joinable(self, joinExprs, twoPlan):
    lhsSchema = twoPlan[0].schema();
    rhsSchema = twoPlan[1].schema();
    for (lField, rField) in joinExprs:
      if lField in lhsSchema.fields and rField in rhsSchema.fields:
        return (lField, rField);
      elif lField in rhsSchema.fields and rField in lhsSchema.fields:
        return (rField, lField);
      else:
        continue;
    return None;
  
  # Helper function to return all the subsets (non-empty, non-full)
  def powerSet(self, iterable):
    xs = list(iterable)
    # note we return an iterator rather than a list
    # We set range(1, len(xs) since we need non-empty, non-full subsets.
    return chain.from_iterable( comb(xs,n) for n in range(1, len(xs)) )
  
  # Our main algorithm - bushy optimizer
  def joinsOptimizer(self, operator, aPaths):
    defaultScaleFactor = 5;
    defaultPartiNumber = 5;
    # build join constraint list;
    joinExprs = self.decodeJoinExprs(operator);
    # build a local plan-cost dict:
    n         = len(aPaths);
    # i = 1
    for aPath in aPaths:
      # Here we define cost by number of pages.
      cards = Plan(root=aPath).sample( defaultScaleFactor );
      pageSize, _, _ = self.db.storage.relationStats(aPath.relationId());
      numPages = cards / (pageSize / aPath.schema().size);
      # Here we only consider reorganize joins
      # so that we simple put accessPaths' totalcost as 0.
      self.addPlanCost(aPath, (numPages, 0));

    for i in range(1, n):
      for S in comb(aPaths, i+1):
        for O in self.powerSet(S):
          (planForO, costL) = self.statsCache[ tuple(sorted(list(map(lambda x : x.id(), O)))) ];
          (remindPl, costR) = self.statsCache[ tuple(sorted(list(map(lambda x:x.id(), [ele for ele in S if ele not in O])))) ];
          fields   = self.joinable(joinExprs, [planForO, remindPl]);
          
          # If we detect constraints, we will create a new join from here.
          if fields is not None:
            lKeySchema = DBSchema('left', [(f, t) for (f, t) in planForO.schema().schema() if f == fields[0]]);
            rKeySchema = DBSchema('right', [(f, t) for (f, t) in remindPl.schema().schema() if f == fields[1]]);
            lHashFn = 'hash(' + fields[0] + ') % ' + str(defaultPartiNumber);
            rHashFn = 'hash(' + fields[1] + ') % ' + str(defaultPartiNumber);
            newJoin = Join(planForO, remindPl, method='hash', \
                           lhsHashFn=lHashFn, lhsKeySchema=lKeySchema, \
                           rhsHashFn=rHashFn, rhsKeySchema=rKeySchema)
            if not self.isRightDeep(newJoin, aPaths):                 
              newJoin.prepare( self.db );
              # Calculate output pages;
              cards = Plan(root=newJoin).sample( defaultScaleFactor );
              pageSize, _, _ = self.db.storage.relationStats(newJoin.relationId());
              pages = cards / (pageSize / newJoin.schema().size);
              # Calculate output costs:
              totalCost = costL[1] + costR[1] + 3 * (costL[0] + costR[0]);
              # Add new Join to self.statsCache
              self.addPlanCost(newJoin, (pages, totalCost));

    return self.getPlanCost(operator)[0];

# We provide a doctest entry main here.
if __name__ == "__main__":
    import doctest
    doctest.testmod()