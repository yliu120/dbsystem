import itertools
from collections import deque

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo

class Optimizer:
  """
  A query optimization class.

  This implements System-R style query optimization, using dynamic programming.
  We only consider left-deep plan trees here.

  >>> import Database
  >>> db = Database.Database()
  >>> try:
  ...   db.createRelation('department', [('did', 'int'), ('eid', 'int')])
  ...   db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  ... except ValueError:
  ...   pass
  ### SELECT * FROM employee JOIN department ON id = eid
  >>> query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid').finalize()

  >>> db.optimizer.pickJoinOrder(query4)

  >>> query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid')\
        .where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
        .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()

  >>> db.optimizer.pushdownOperators(query5)

  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    raise NotImplementedError

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    raise NotImplementedError

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  # (Algorithm)
  #    Basically, we consider two situations
  #    i)  Alias of fields would be created during projection
  #    ii) Some fields may be projected out prior to join operators if they
  #        are directly pushed down.
  #    To resolve i) and ii),
  #    i) We traverse the tree to grab out all the fields and their alias.
  #    ii) When we visit a join operator, we marked the join conditions as
  #        required and we carry these information down
  #    
  #    After we correctly deal with i) and ii)
  #    For each relation (Tablescan), we invoke cost models to compute the
  #    selectivity of expressions regarding to the same field. We filter out
  #    some unnecessary expressions. Meanwhile, we will consider whether to serve
  #    selection or projection first for each relation.
  
  #    throughout the entire algo, we will keep some data structures.
  
  # Helper functions
  
  # correcting all the unary operator and match each operator to one relation
  def correctUnary(self, plan):
    selectExprs  = [];
    projections = [];
    tableScans  = dict();    
    # traverse the plan tree
    unarys      = plan.sources;
    
    # sort out all different unary operators
    for op in unarys:
      if op.operatorType() == "TableScan":
        if op.relId not in tableScans:
          tableScans[ op.relId ] = op.schema().fields;
      elif op.operatorType() == "Select":
        selectExprs.append( op.selectExpr );
      else:
        projections.append( op );
     
    # build fields alias:
    fieldAlias = buildAlias( projections, tableScans );   
    # process expressions for selections
    modifiedSel = modifySelect( selectExprs, fieldAlias );
    # ...
    
  # This function returns a fieldAlias reference dictionary
  def buildAlias(self, projlst, tableLst):
    
      
     
     
  def pushdownOperators(self, plan):
    raise NotImplementedError

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    raise NotImplementedError

  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(plan)

    return joinPicked_plan

if __name__ == "__main__":
  import doctest
  doctest.testmod()
