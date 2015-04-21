import itertools
from collections import deque

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo
from Catalog.Schema import DBSchema

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

  def pushdownProjections(self, operator):
    
    if operator.operatorType() == "TableScan":
      return operator;
    elif ( operator.operatorType() == "Select" or operator.operatorType() == "GroupBy") :
      newSubPlan  = self.pushdownProjections( operator.subPlan );
      operator.subPlan = newSubPlan;
      return operator;
    elif ( operator.operatorType() == "UnionAll" or operator.operatorType()[-4:] == "Join" ):
      newlPlan = self.pushdownProjections( operator.lhsPlan );
      newrPlan = self.pushdownProjections( operator.rhsPlan );
      operator.lhsPlan = newlPlan;
      operator.rhsPlan = newrPlan;
      return operator;
    else:
      subPlan = operator.subPlan;
      if subPlan.operatorType() == "TableScan":
        return operator;
      elif subPlan.operatorType() == "Select":
        subSubPlan = subPlan.subPlan;
        operator.subPlan = subSubPlan;
        subPlan.subPlan = operator;
        return self.pushdownProjections( subPlan );
      elif subPlan.operatorType() == "GroupBy":
        newSubSubPlan = self.pushdownProjections( subPlan.subPlan );
        subPlan.subPlan = newSubSubPlan;
        return operator;
      elif subPlan.operatorType() == "Project":
        # Note that here we need to combine two projections
        # We assume that the upper projection must be based on the outputschema
        # of the lower one;
        subRepExp = { k : v1 for (k, (v1, _)) in subPlan.projectExprs.items() };
        newExpr   = dict();
        
        # Combine projections
        # TODO: Here we don't guarantee 100% success of replacement
        for (k, (v1, v2)) in operator.projectExprs.items():
          newV1 = v1
          for (key, value) in subRepExp:
            newV1 = newV1.replace(key, value);
          newExpr[k] = (newV1, v2);
        
        # Reorder the projection operator
        operator.projectExprs  = newExpr;
        operator.outputSchema  = DBSchema(operator.relationId(), \
                          [(k, v[1]) for (k,v) in operator.projectExprs.items()])
        operator.subPlan      = subPlan.subPlan;
        return self.pushdownProjections( operator );
      elif subPlan.operatorType() == "UnionAll":
        # For Union operator, the push down is very simple
        subPlan.lhsPlan = Project(subPlan.lhsPlan, operator.projectExprs);
        subPlan.rhsPlan = Project(subPlan.rhsPlan, operator.projectExprs);
        subPlan.validateSchema();
        del operator;
        return self.pushdownProjections( subPlan );
      else:
        # Here we deal with the Join Case
        # This is a lot harder than other cases
        # The first step is to collect input fields needed directly.
        # We grab out the fields in the projectExprs first
        # and then filter them with the project inputSchema
        fields = set();
        outputNames = [k for (k, (v1, _)) in operator.projectExprs.items()];
        inputNames  = operator.inputSchemas()[0].fields;
        lhsPlanNames= subPlan.lhsPlan.schema().fields;
        rhsPlanNames= subPlan.rhsPlan.schema().fields;

        for (k, (v1, _)) in operator.projectExprs.items():
          attributes = ExpressionInfo( v1 ).getAttributes();
          # filter attributes
          for name in attributes:
            if name not in inputNames:
              attributes.remove( name );
          fields = fields.union( attributes );
              
        # collecting join condition fields;
        if subPlan.joinMethod == "nested-loops" or subPlan.joinMethod == "block-nested-loops":
          fields = fields.union( ExpressionInfo( subPlan.joinExpr ).getAttributes() );
        elif subPlan.joinMethod == "hash":
          fields = fields.union( set( subPlan.lhsKeySchema.fields + subPlan.rhsKeySchema.fields ) );
        else:
          # We don't support indexed
          raise NotImplementedError;
      
        # constructing virtual l and r projections
        lprojectExpr = dict();
        rprojectExpr = dict();
        for (f, v) in subPlan.lhsPlan.schema().schema():
          if f in fields:
            lprojectExpr[ f ] = (f, v);
        for (f, v) in subPlan.rhsPlan.schema().schema():
          if f in fields:
            rprojectExpr[ f ] = (f, v);
        
        if len(lprojectExpr) != len(lhsPlanNames):
          subPlan.lhsPlan = Project( subPlan.lhsPlan, lprojectExpr );
          subPlan.lhsPlan.outputSchema  = DBSchema(subPlan.lhsPlan.relationId(), \
                          [(k, v[1]) for (k,v) in subPlan.lhsPlan.projectExprs.items()])

        if len(rprojectExpr) != len(rhsPlanNames):
          subPlan.rhsPlan = Project( subPlan.rhsPlan, rprojectExpr );
          subPlan.rhsPlan.outputSchema  = DBSchema(subPlan.rhsPlan.relationId(), \
                          [(k, v[1]) for (k,v) in subPlan.rhsPlan.projectExprs.items()])
          
        if subPlan.validateJoin():
          subPlan.initializeSchema();
        # push down project through join
        operator.subPlan = self.pushdownProjections( subPlan );
        return operator;

  def pushdownSelections(self, operator):
    if operator.operatorType() == "TableScan":
      return operator;
    elif ( operator.operatorType() == "Project" or operator.operatorType() == "GroupBy") :
      newSubPlan  = self.pushdownSelections( operator.subPlan );
      operator.subPlan = newSubPlan;
      return operator;
    elif ( operator.operatorType() == "UnionAll" or operator.operatorType()[-4:] == "Join" ):
      newlPlan = self.pushdownSelections( operator.lhsPlan );
      newrPlan = self.pushdownSelections( operator.rhsPlan );
      operator.lhsPlan = newlPlan;
      operator.rhsPlan = newrPlan;
      return operator;
    else:
      return operator;

  def pushdownOperators(self, plan):
      
    if plan.root:
      newroot = self.pushdownProjections( plan.root );
      ultroot = self.pushdownSelections( newroot );
      plan.root = newroot;
      return plan;
    else:
      raise ValueError("An Empty Plan cannot be optimized.")

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
