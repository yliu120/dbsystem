from Catalog.Schema import DBSchema
from Catalog.Identifiers import TupleId
from Query.Operator import Operator
from heapq import heappush, heappop

import gc;

# Operator for External Sort
class Sort(Operator):

  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)
    self.subPlan     = subPlan
    self.sortKeyFn   = kwargs.get("sortKeyFn", None)
    self.sortKeyDesc = kwargs.get("sortKeyDesc", None)

    if self.sortKeyFn is None or self.sortKeyDesc is None:
      raise ValueError("No sort key extractor provided to a sort operator")
    
    self.tmpFileMap = dict();

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
      
    self.inputIterator = iter(self.subPlan);
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
    bufPool       = self.storage.bufferPool;
    passId        = 0;
    runId         = 0;
    inputFinished = False;
    
    self.cleanBufferPool(bufPool);
    # pass 0
    while( not(inputFinished) ):
      # The algorithm should be like this.
      # From pass 0, we sort B-1 page and do a B-1-way merge
      # But we need to keep one output page in the bufferpool
      while( bufPool.numFreePages() > 1 ):
        try:
          (pageId, page) = next(self.inputIterator);
          bufPool.getPage( pageId, True );
        except StopIteration:
          inputFinished = True;
          break;
        
      tmpFile = self.getTmpFile(passId, runId);
      # here the heap has size with B-1, in place
      pageIterators = [];
      orderId       = 0 ;
      for (pageId, (_, page, _)) in bufPool.pageMap.items():
        #self.pageSort(page, pageId);
        pageIterators.append( (self.pageSort(page, pageId), orderId) );
        orderId    += 1 ;
    
      self.kWayMergeOutput(pageIterators, tmpFile);
      
      for (pageId, (_, page, pinCount)) in bufPool.pageMap.items():
        if pinCount > 0:
          bufPool.unpinPage( pageId );
          page.setDirty(False);
        
      self.cleanBufferPool(bufPool);
      runId += 1;
      
    # Check ready for output
    # if true then return the output iterator
    # if false then goto pass 1...N
    # implementing pass 1 ... N
    while( not(self.isOutputReady( passId )) ):
      
      listIterator  = iter( self.tmpFileMap[ passId ] );
      passId       += 1;
      runId         = 0;
      while( not(listIterator is None) ):
        # implementing runs 0 ... M 
        fileIterators = dict();
        # Pull corresponding files to the buffer pool 
        # orderId is to make the sorting algorithm stable 
        orderId       = 0; 
        while( bufPool.numFreePages() > 1 ):
          try:
            fileIterator  = self.storage.fileMgr.relationFile( next(listIterator) )[1].pages();
          except StopIteration:
            listIterator = None;
            break;
        
          (pId, firstPage)     = next( fileIterator );
          firstPageIter        = iter( firstPage ); 
          bufPool.getPage( pId, True );
          fileIterators[ ( pId, firstPageIter, orderId) ] = fileIterator;
          
          orderId      += 1;
        
        # run k-way merge sort for this run
        tmpFile = self.getTmpFile( passId, runId );
        self.kWayMergeOutputWithFile(bufPool, fileIterators, tmpFile);
        
        # End run
        runId += 1;
        # End run cleanup
        self.cleanBufferPool(bufPool);
      # End pass
      # End pass cleanup
      for relTmp in self.tmpFileMap[ passId - 1 ]:
        self.storage.fileMgr.removeRelation( relTmp );
      del self.tmpFileMap[ passId - 1 ];
      del listIterator;
     
    return self.storage.pages(self.tmpFileMap[passId][0]);  
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
    for tuple in sorted(pageIterator, key = lambda e : self.sortKeyFn(schema.unpack(e)) ):
      yield tuple;
      
  # Another helper function that borrowed from Join.py
  # We need to clean buffer pool before using
  # clean buffer pool before use
  def cleanBufferPool(self, bufPool):

    # evict out clean pages and flush dirty pages
    for (pageId, (_, page, pinCount)) in bufPool.pageMap.items():
      if not(pinCount == 0):
        raise RuntimeError("Unable to clean bufferpool. Memory leaks?" + "(PageId: " \
                           + str(pageId.pageIndex) + " pinCount: " + str(pinCount));
      else:
        if (page.isDirty()):
          # evict with flush
          bufPool.flushPage( pageId );
        # evict without flush
        bufPool.discardPage( pageId );
        
  def getTmpFile(self, passId, runId):
    
    if self.sortKeyDesc:
      relIdTmp = self.relationId() + "_" + self.sortKeyDesc + "_" + str(passId) + "+" + str(runId);
    else:
      relIdTmp = self.relationId() + "_" + "sort" + "_" + str(passId) + "+" + str(runId);
    
    if not(self.storage.hasRelation(relIdTmp)):
      self.storage.createRelation(relIdTmp, self.subPlan.schema());
      tempFile = self.storage.fileMgr.relationFile(relIdTmp)[1];
      
      if passId in self.tmpFileMap:
        self.tmpFileMap[ passId ].append(relIdTmp);
      else:
        self.tmpFileMap[ passId ] = [ relIdTmp ];
        
    return self.storage.fileMgr.relationFile(relIdTmp)[1];

  def kWayMergeOutput(self, pageIterators, outputFile):
    
    heap    = [];
    schema  = self.subPlan.schema();
    
    # redefine the function locally
    sortKeyFnTuple = lambda e : self.sortKeyFn(schema.unpack(e));
    for p in pageIterators:
      tuple = next(p[0]);
      heappush(heap, ( sortKeyFnTuple( tuple ), p[1], tuple, p[0] ));
    
    while ( heap != [] ):
      # Add an order to make the sorting stable;      
      (value, order, tupleData, g) = heappop(heap);
      try:
        nextTuple = next(g);
        heappush(heap, ( sortKeyFnTuple( nextTuple ), order, nextTuple, g ));
      except StopIteration:
        pass
    
      outputFile.insertTuple( tupleData );

    # clean up heap
    del heap;
  
  # Here we provide a k-way merge output for pass 1, 2, 3.. N
  # This function differs from the previous one by including file iterators
  # bufpool: the bufferpool associated with this operator
  # fileIterators: in form: <(firstPageTupleIter, orderId), fileIterator>
  # outputFile   : the output
  def kWayMergeOutputWithFile(self, bufPool, fileIterators, outputFile):
    
    heap    = [];
    schema  = self.subPlan.schema();
    # redefine the function locally
    sortKeyFnTuple = lambda e : self.sortKeyFn(schema.unpack(e));
    
    # initialize heap
    for (pageId, tupleIter, orderId) in fileIterators.keys():
      tuple = next(tupleIter);
      heappush(heap, ( sortKeyFnTuple( tuple ), orderId, tuple, tupleIter, pageId ) );
    
    while( heap != [] ):
      
      (key, order, tupleData, tupleIter, pageId) = heappop(heap);  
      try:
        nextTuple = next( tupleIter );
        heappush(heap, ( sortKeyFnTuple( nextTuple ), order, nextTuple, tupleIter, pageId ));
      except StopIteration:
          
        bufPool.unpinPage( pageId );
        bufPool.discardPage( pageId );
        fileIter  = fileIterators[(pageId, tupleIter, order)];
        
        # We open up a new page
        try:
          (nextpId, nextPage) = next( fileIter );
          nextIter            = iter( nextPage );
          # We add the nextPage to our dictionary and buffer pool.
          fileIterators[(nextpId, nextIter, order)] = fileIter;
          del fileIterators[(pageId, tupleIter, order)];
          # pin the new next page in the bufferpool
          bufPool.pinPage( nextpId );
          # keep our heap structure
          nextTuple = next( nextIter );
          heappush(heap, ( sortKeyFnTuple( nextTuple ), order, nextTuple, nextIter, nextpId ) );
        except StopIteration:
          pass
    
      outputFile.insertTuple( tupleData );

    # clean up
    del heap;
      
  def isOutputReady(self, passId):
    return True if len(self.tmpFileMap[passId]) == 1 else False;