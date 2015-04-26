from Query.Optimizer import Optimizer

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo
from Catalog.Schema import DBSchema

from DBFileSystemGC import DBFileSystemGC

# This optimizer consider all bushy trees
# greedily constructs plans using the cheapest
# join available over the subplans.
class GreedyOptimizer(Optimizer):
  
  """
  A query optimization class which implements the dynamic programming optimizer
  algorithm in a greedy way. In this algorithm, we consider any form of the join
  plan, such as left-deep plan, bushy-plan and right deep plan. 
  
  For performance consideration, we do not provide any real dataset tests for
  this implementation. By the following doctests, we provide an artificial test
  for this optimizer.

  >>> import Database
  >>> from Query.Optimizer import Optimizer
  >>> from Catalog.Schema import DBSchema
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

  >>> db.optimizer = GreedyOptimizer(db);
  >>> db.optimizer.pickJoinOrder(query);
  >>> db.removeRelation('department');
  >>> db.removeRelation('employee');
  >>> db.removeRelation('project');
  >>> db.removeRelation('grant');
  >>> db.close();

  """
  def __init__(self, db):
    super().__init__(db);
    self.pcntr = 0;

  # Helper function to test whether two plans are joinable
  def joinable(self, operator, twoPlan):
    joinExprs = self.decodeJoinExprs(operator);
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

  # Our main algorithm - greedy optimizer
  def joinsOptimizer(self, operator, aPaths):
    defaultScaleFactor = 5;
    defaultPartiNumber = 5;

    n = len(aPaths);
    planList = [];
    costList = [];
    # i = 1
    for aPath in aPaths:
      # Here we define cost by number of pages.
      cards = Plan(root=aPath).sample( defaultScaleFactor );
      pageSize, _, _ = self.db.storage.relationStats(aPath.relationId());
      numPages = cards / (pageSize / aPath.schema().size);
      # Here we only consider reorganize joins
      # so that we simple put accessPaths' total cost as 0.
      planList.append(aPath);
      costList.append((numPages, 0));
    # i = 2...n
    for i in range(1, n):
      # find all possible two way join in current planList
      # put the potential joins in potentialP
      # put the potential joins cost in potentialC
      m = len(planList);
      potentialP = [];
      potentialC = [];
      for j in range(0, m-1):
        for k in range(j+1, m):
          self.pcntr += 1;
          potentialP.append((planList[j], planList[k]));
          potentialC.append(3*(costList[j][0] + costList[k][0]) + costList[j][1] + costList[k][1]);
      # find the cheapest joinable join (total cost)
      # build the join, remove the used two base plan and add the new join to planList
      # modify the costList as well
      while(potentialC):
        currC = min(potentialC);
        currP = potentialP[potentialC.index(currC)];
        potentialC.remove(currC);
        potentialP.remove(currP);
        if(self.joinable(operator, currP)):
          (lField, rField) = self.joinable(operator, currP);
          lhsSchema = currP[0].schema();
          rhsSchema = currP[1].schema();
          lKeySchema = DBSchema('left', [(f, t) for (f, t) in lhsSchema.schema() if f == lField]);
          rKeySchema = DBSchema('right', [(f, t) for (f, t) in rhsSchema.schema() if f == rField]);
          lHashFn = 'hash(' + lField + ') % ' + str(defaultPartiNumber);
          rHashFn = 'hash(' + rField + ') % ' + str(defaultPartiNumber);
          newJoin = Join(currP[0], currP[1], method='hash', \
                         lhsHashFn=lHashFn, lhsKeySchema=lKeySchema, \
                         rhsHashFn=rHashFn, rhsKeySchema=rKeySchema)
                             
          newJoin.prepare( self.db );
          totalCost = currC;
          cards = Plan(root=newJoin).sample( defaultScaleFactor );
          pageSize, _, _ = self.db.storage.relationStats(newJoin.relationId());
          pages = cards / (pageSize / newJoin.schema().size);
          
          id1 = planList.index(currP[0]);
          _ = planList.pop(id1);
          id2 = planList.index(currP[1]);
          _ = planList.pop(id2);
          planList.append(newJoin);
          _ = costList.pop(id1);
          _ = costList.pop(id2);
          costList.append((pages, totalCost));
          break;
    print ("GreedyOptimizer plan considered: ", self.pcntr);
    return planList[0]

# We provide a doctest entry main here.
if __name__ == "__main__":
    import doctest
    doctest.testmod()