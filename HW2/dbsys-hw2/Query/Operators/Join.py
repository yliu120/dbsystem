import itertools

from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Join(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined join operator not supported")

    self.lhsPlan    = lhsPlan
    self.rhsPlan    = rhsPlan
    self.joinExpr   = kwargs.get("expr", None)
    self.joinMethod = kwargs.get("method", None)
    self.lhsSchema  = kwargs.get("lhsSchema", None if lhsPlan is None else lhsPlan.schema())
    self.rhsSchema  = kwargs.get("rhsSchema", None if rhsPlan is None else rhsPlan.schema())
    
    self.lhsKeySchema   = kwargs.get("lhsKeySchema", None)
    self.rhsKeySchema   = kwargs.get("rhsKeySchema", None)
    self.lhsHashFn      = kwargs.get("lhsHashFn", None)
    self.rhsHashFn      = kwargs.get("rhsHashFn", None)

    self.validateJoin()
    self.initializeSchema()
    self.initializeMethod(**kwargs)

  # Checks the join parameters.
  def validateJoin(self):
    # Valid join methods: "nested-loops", "block-nested-loops", "indexed", "hash"
    if self.joinMethod not in ["nested-loops", "block-nested-loops", "indexed", "hash"]:
      raise ValueError("Invalid join method in join operator")

    # Check all fields are valid.
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      methodParams = [self.joinExpr]
    
    elif self.joinMethod == "indexed":
      methodParams = [self.lhsKeySchema] 
    
    elif self.joinMethod == "hash":
      methodParams = [self.lhsHashFn, self.lhsKeySchema, \
                      self.rhsHashFn, self.rhsKeySchema]
    
    requireAllValid = [self.lhsPlan, self.rhsPlan, \
                       self.joinMethod, \
                       self.lhsSchema, self.rhsSchema ] \
                       + methodParams

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete join specification, missing join operator parameter")

    # For now, we assume that the LHS and RHS schema have
    # disjoint attribute names, enforcing this here.
    for lhsAttr in self.lhsSchema.fields:
      if lhsAttr in self.rhsSchema.fields:
        raise ValueError("Invalid join inputs, overlapping schema detected")


  # Initializes the output schema for this join. 
  # This is a concatenation of all fields in the lhs and rhs schema.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.lhsSchema.schema() + self.rhsSchema.schema()
    self.joinSchema = DBSchema(schema, fields)

  # Initializes any additional operator parameters based on the join method.
  def initializeMethod(self, **kwargs):
    if self.joinMethod == "indexed":
      self.indexId = kwargs.get("indexId", None)
      if self.indexId is None or self.lhsKeySchema is None \
          or self.storage.getIndex(self.indexId) is None:
        raise ValueError("Invalid index for use in join operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.joinSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.lhsSchema, self.rhsSchema]

  # Returns a string describing the operator type
  def operatorType(self):
    readableJoinTypes = { 'nested-loops'       : 'NL'
                        , 'block-nested-loops' : 'BNL' 
                        , 'indexed'            : 'Index'
                        , 'hash'               : 'Hash' }
    return readableJoinTypes[self.joinMethod] + "Join"

  # Returns child operators if present
  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for join operator.
  def __iter__(self):
    self.initializeOutput();
    self.inputIteratorL = iter(self.lhsPlan);
    self.inputIteratorR = iter(self.rhsPlan);
    self.outputIterator = self.processAllPages();

    return self;

  def __next__(self):
    return next(self.outputIterator);

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.joinMethod == "nested-loops":
      return self.nestedLoops()
    
    elif self.joinMethod == "block-nested-loops":
      return self.blockNestedLoops()

    elif self.joinMethod == "indexed":
      return self.indexedNestedLoops()
    
    elif self.joinMethod == "hash":
      return self.hashJoin()

    else:
      raise ValueError("Invalid join method in join operator")


  ##################################
  #
  # Nested loops implementation
  #
  def nestedLoops(self):
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        # Load the lhs once per inner loop.
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

        for (rPageId, rhsPage) in iter(self.rhsPlan):
          for rTuple in rhsPage:
            # Load the RHS tuple fields.
            joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

            # Evaluate the join predicate, and output if we have a match.
            if eval(self.joinExpr, globals(), joinExprEnv):
              outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())



  ##################################
  #
  # Block nested loops implementation
  #
  # This attempts to use all the free pages in the buffer pool
  # for its block of the outer relation.

  def blockNestedLoops(self):
    print ('BNLJ called.');
    for pageBlock in self.accessPageBlock(self.storage.bufferPool, self.inputIteratorL):
      for lPageId in pageBlock:
        lhsPage = self.storage.bufferPool.getPage(lPageId);
        for lTuple in lhsPage:
          joinExprEnv = self.loadSchema(self.lhsSchema, lTuple);
          #print (joinExprEnv);     
          
          for (rPageId, rhsPage) in self.inputIteratorR:
              #print ("rPage loop");
            for rTuple in rhsPage:
              joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple));
                #print (joinExprEnv);
              
              if eval(self.joinExpr, globals(), joinExprEnv):
                outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields]);
                  #self.emitOutputTuple(self.joinSchema.pack(outputTuple));
                outputTupleP = self.joinSchema.pack(outputTuple);
                self.storage.fileMgr.relationFile(self.relationId())[1].insertTuple(outputTupleP);
          
            #print ("no more inputIteratorR");
            
        
        self.storage.bufferPool.unpinPage(lPageId);
        self.storage.bufferPool.discardPage(lPageId);
    
    return self.storage.pages(self.relationId());

  # Accesses a block of pages from an iterator.
  # This method pins pages in the buffer pool during its access.
  # We track the page ids in the block to unpin them after processing the block.
  def accessPageBlock(self, bufPool, pageIterator):
    print ("accessPageBlock called");
    self.cleanBufferPool( bufPool );
    pageBlock = [];
    self.inputFinished = False;
    while not(self.inputFinished):
      try:
        (pageId, page) = next(pageIterator);
        if (bufPool.numFreePages() > 2):
          _ = bufPool.getPage(pageId);
          bufPool.pinPage(pageId);
          pageBlock.append(pageId);
        else:
          yield pageBlock;
          pageBlock = [];
      except StopIteration:
        self.inputFinished = True;
        yield pageBlock;
      


  ##################################
  #
  # Indexed nested loops implementation
  #
  def indexedNestedLoops(self):
    raise NotImplementedError

  ##################################
  #
  # Some helper function
  #
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
  
  ##################################
  #
  # Hash join implementation.
  #
  def hashJoin(self):
    if self.joinExpr == None:
      self.joinExpr = self.lhsKeySchema.fields[0] + "==" + self.rhsKeySchema.fields[0];
      # what if KeySchema includes multiple fields?
      # what if joinExpr is a range comparison?
    
    self.tmpFilesL = list();
    self.tmpFilesR = list();
    
    for (PageId, Page) in self.inputIteratorL:
      self.buildPartitionL(PageId, Page);
    for (PageId, Page) in self.inputIteratorR:
      self.buildPartitionR(PageId, Page);
      
    for relIdTmpL in self.tmpFilesL:
      relIdTmpR = relIdTmpL.rstrip('L') + 'R';
      
      if (self.storage.hasRelation(relIdTmpR)):
        self.inputIteratorL = self.storage.pages(relIdTmpL);
        self.inputIteratorR = self.storage.pages(relIdTmpR);
        _ = self.blockNestedLoops();
        
        self.storage.removeRelation(relIdTmpL);
        self.storage.removeRelation(relIdTmpR);
    
    return self.storage.pages(self.relationId());
        

  def buildPartitionL(self, PageId, Page):
    inputSchema  = self.lhsSchema;
    for inputTuple in Page:
      inputL = self.loadSchema(inputSchema, inputTuple);
      
      relIdTmp = self.relationId() + str(eval(self.lhsHashFn, globals(), inputL)) + "HashJoinTmpL";
      
    
      if not(self.storage.hasRelation(relIdTmp)):
        self.storage.createRelation(relIdTmp, inputSchema);
        tempFile = self.storage.fileMgr.relationFile(relIdTmp)[1];
        tempFile.insertTuple( inputTuple );
        self.tmpFilesL.append(relIdTmp);
      
      else:
        tempFile = self.storage.fileMgr.relationFile(relIdTmp)[1];
        tempFile.insertTuple( inputTuple );

  def buildPartitionR(self, PageId, Page):
    inputSchema  = self.rhsSchema;
    for inputTuple in Page:
      inputR = self.loadSchema(inputSchema, inputTuple);
      
      relIdTmp = self.relationId() + str(eval(self.rhsHashFn, globals(), inputR)) + "HashJoinTmpR";
      
      
      if not(self.storage.hasRelation(relIdTmp)):
        self.storage.createRelation(relIdTmp, inputSchema);
        tempFile = self.storage.fileMgr.relationFile(relIdTmp)[1];
        tempFile.insertTuple( inputTuple );
        self.tmpFilesR.append(relIdTmp);
      
      else:
        tempFile = self.storage.fileMgr.relationFile(relIdTmp)[1];
        tempFile.insertTuple( inputTuple );



  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      exprs = "(expr='" + str(self.joinExpr) + "')"

    elif self.joinMethod == "indexed":
      exprs =  "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "indexKeySchema=" + self.lhsKeySchema.toString() ]
        ))) + ")"

    elif self.joinMethod == "hash":
      exprs = "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "lhsKeySchema=" + self.lhsKeySchema.toString() ,
            "rhsKeySchema=" + self.rhsKeySchema.toString() ,
            "lhsHashFn='" + self.lhsHashFn + "'" ,
            "rhsHashFn='" + self.rhsHashFn + "'" ]
        ))) + ")"
    
    return super().explain() + exprs

  # Join costs must consider the cost of matching all pairwise inputs.
  def cost(self):
    numMatches = self.lhsPlan.cost() * self.rhsPlan.cost()
    return self.selectivity() * numMatches

  # This should be computed based on statistics collection.
  # For now, we simply return a constant.
  def selectivity(self):
    return 1.0

