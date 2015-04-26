from Query.Optimizer import Optimizer

# This optimizer consider all bushy trees
# greedily constructs plans using the cheapest
# join available over the subplans.
class GreedyOptimizer(Optimizer):
    
  def __init__(self, db):
    super().__init__(db);

  # revised version for bushy-trees
  def decodeJoins(self, operator):
    if operator:
      lst = [];
      if operator.operatorType()[-4:] == "Join":
        if operator.lhsPlan.operatorType()[-4:] == "Join":
          lst += self.decodeJoins(operator.lhsPlan);
        else:
          lst.append(operator.lhsPlan);
        if operator.rhsPlan.operatorType()[-4:] == "Join":
          lst += self.decodeJoins(operator.rhsPlan);
        else: 
          lst.append(operator.rhsPlan);
        return lst;
    else:
      raise ValueError("Empty plan cannot be decoded.");

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
    defaultScaleFactor = 50;
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
      for j in range(0, m-1):
        for k in range(j+1, m):
          potentialP.append((planList[j], planList[k]));
          potentialC.append(3*(costList[j][0] + costList[k][0]) + costList[j][1] + costList[k][1]);
      while(potentialC):
        currC = min(potentialC);
        currP = potentialP[potentialC.index(currC)];
        poteintialC.remove(currC);
        potientialP.remove(currP);
        if(self.joinable(operator, currP)):
          (lField, rField) = self.joinable(operator, currP);
          lhsSchema = currP[0].schema();
          rhsSchema = currP[1].schema();
          lKeySchema = DBSchema('left', [(f, t) for (f, t) in lhsSchema.schema() if f == lField]);
          rKeySchema = DBSchema('right', [(f, t) for (f, t) in rhsSchema.schema() if f == rField]);
          lHashFn = 'hash(' + lField + ') % ' + str(defaultPartiNumber);
          rHashFn = 'hash(' + rField + ') % ' + str(defaultPartiNumber);
          newJoin = Join(p, rPlan, method='hash', \
                         lhsHashFn=lHashFn, lhsKeySchema=lKeySchema, \
                         rhsHashFn=rHashFn, rhsKeySchema=rKeySchema)
                             
          newJoin.prepare( self.db );
          totalCost = currC;
          cards = Plan(root=newJoin).sample( defaultScaleFactor );
          pageSize, _, _ = self.db.storage.relationStats(newJoin.relationId());
          pages = cards / (pageSize / newJoin.schema().size);
          
          id1 = planList.index(currP[0]);
          id2 = planList.index(currP[1]);
          _ = planList.pop(id1);
          _ = planList.pop(id2);
          planList.append(newJoin);
          _ = costList.pop(id1);
          _ = costList.pop(id2);
          costList.append((pages, totalCost));
          break;
        
    return planList[0]
