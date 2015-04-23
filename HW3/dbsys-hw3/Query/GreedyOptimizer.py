from Query.Optimizer import Optimizer

# This optimizer consider all bushy trees
# greedily constructs plans using the cheapest
# join available over the subplans.
class BushyOptimizer(Optimizer):
    
  def __init__(self, db):
    super().__init__(db);

  # Our main algorithm - greedy optimizer
  def joinsOptimizer(self, operator, aPaths):
    return operator;