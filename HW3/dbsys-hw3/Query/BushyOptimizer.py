from Query.Optimizer import Optimizer

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

  # Our main algorithm - bushy optimizer
  def joinsOptimizer(self, operator, aPaths):
    return operator;