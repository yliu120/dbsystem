from Query.Optimizer import Optimizer

# This optimizer consider all bushy trees
class BushyOptimizer(Optimizer):
    
  def __init__(self, db):
    super().__init__(db);

  # Our main algorithm - bushy optimizer
  def joinsOptimizer(self, operator, aPaths):
    return operator;