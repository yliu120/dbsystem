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
    
    # local variable
    schema  = self.schema();
    
    # perpare bufferpool
    bufpool = self.storage.bufferpool;
    self.cleanBufferPool(bufPool);
    
    passId  = 0;
    runId   = 0;
    # pass 0
    while( self.inputIterator ):
      
      (pageId, page) = next(self.inputIterator);
      while( bufpool.numFreePages() > 0 ):
        bufpool.getPage( pageId, True );
      
      for (pageId, (_, page, _)) in self.pageMap.items():
        self.pageSort(page);
      
      
      
      
    


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
  def pageSort(self, page, pageId):
    
    pageIterator = iter(page);
    schema       = self.subPlan.schema();
    tmpList      = sorted(pageIterator, key = lambda e : self.sortKeyFn(schema.unpack(e)) );
    
    id = 0;
    for tupleData in tmpList:
      page.putTuple( TupleId(pageId, id), tupleData );
      id += 1;
      
  # Another helper function that borrowed from Join.py
  # We need to clean buffer pool before using
  # clean buffer pool before use
  def cleanBufferPool(self, bufPool):

    # evict out clean pages and flush dirty pages
    for (pageId, (_, page, pinCount)) in bufPool.pageMap.items():
      if not(pinCount == 0):
        raise RuntimeError("Unable to clean bufferpool. Memory leaks?");
      else:
        if (page.isDirty()):
          # evict with flush
          bufPool.flushPage( pageId );
        # evict without flush
        bufPool.discardPage( pageId );
