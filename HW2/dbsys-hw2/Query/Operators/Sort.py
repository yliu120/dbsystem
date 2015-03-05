from Catalog.Schema import DBSchema
from Query.Operator import Operator

# Operator for External Sort
class Sort(Operator):

  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)
    self.subPlan     = subPlan
    self.sortKeyFn   = kwargs.get("sortKeyFn", None)
    self.sortKeyDesc = kwargs.get("sortKeyDesc", None)

    if self.sortKeyFn is None or self.sortKeyDesc is None:
      raise ValueError("No sort key extractor provided to a sort operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.subPlan.schema()

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "Sort"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]


  # Iterator abstraction for external sort operator.
  # Apparently, external sort must not support page-at-a-time
  # We apply a similar iterator implementation here as GroupBy
  def __iter__(self):
      
    self.initializeOutput();
    self.inputIterator = iter(self.subPlan);
    self.inputFinished = False;
    
    self.outputIterator = self.processAllPages();
    
    return self

  def __next__(self):  
    return next(self.outputIterator);


  # Page processing and control methods

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise NotImplementedError

  # Set-at-a-time operator processing
  def processAllPages(self):
    raise NotImplementedError


  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(sortKeyDesc='" + str(self.sortKeyDesc) + "')"

  def cost(self):
    return self.selectivity() * self.subPlan.cost()

  def selectivity(self):
    return 1.0

  # This function is a helper function on pass 0. 
  # This is not an in-place version...
  def pageSort(self, page):
    
    pageIterator = iter(page);
    pageId       = page.pageId;
    schema       = self.subPlan.schema();
    tmpList      = sorted(pageIterator, key = lambda e : self.sortKeyFn(schema.unpack(e)) );
    
    id = 0;
    for tupleData in tmpList:
      page.putTuple( TupleId(pageId, id), tupleData );
      id += 1;
