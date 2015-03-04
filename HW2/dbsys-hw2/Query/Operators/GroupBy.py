from Catalog.Schema import DBSchema
from Query.Operator import Operator

import gc, sys;
import collections
from _functools import reduce

class GroupBy(Operator):
    
  '''
          query6 = db.query().fromTable('employee').groupBy( \
          groupSchema=keySchema, \
          aggSchema=aggMinMaxSchema, \
          groupExpr=(lambda e: e.id), \
          aggExprs=[(sys.maxsize, lambda acc, e: min(acc, e.age), lambda x: x), \
                    (0, lambda acc, e: max(acc, e.age), lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal[0]) % 2) \
          ).finalize()
  '''
  memoryLimit = 1 << 20; # 1GB   
    
  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined group-by-aggregate operator not supported")

    self.subPlan     = subPlan
    self.subSchema   = subPlan.schema()
    self.groupSchema = kwargs.get("groupSchema", None)
    self.aggSchema   = kwargs.get("aggSchema", None)
    self.groupExpr   = kwargs.get("groupExpr", None)
    self.aggExprs    = kwargs.get("aggExprs", None)
    self.groupHashFn = kwargs.get("groupHashFn", None)

    self.validateGroupBy()
    self.initializeSchema()
    self.tmpFiles    = dict();

  # Perform some basic checking on the group-by operator's parameters.
  def validateGroupBy(self):
    requireAllValid = [self.subPlan, \
                       self.groupSchema, self.aggSchema, \
                       self.groupExpr, self.aggExprs, self.groupHashFn ]

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete group-by specification, missing a required parameter")

    if not self.aggExprs:
      raise ValueError("Group-by needs at least one aggregate expression")

    if len(self.aggExprs) != len(self.aggSchema.fields):
      raise ValueError("Invalid aggregate fields: schema mismatch")

    self.isGroupExprResultHashable();
    self.running = self.isAggExprRunning();

  # Initializes the group-by's schema as a concatenation of the group-by
  # fields and all aggregate fields.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    self.outputSchema = DBSchema(schema, fields)

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "GroupBy"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for group-by operator.
  # Note that the Group-By operator does NOT support pipeline mode
  def __iter__(self):
      
    self.initializeOutput();
    self.inputIterator = iter(self.subPlan);
    self.inputFinished = False;
    
    self.outputIterator = self.processAllPages();
    
    return self

  def __next__(self):  
    return next(self.outputIterator);

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    
    # Add local schemas
    inputSchema  = self.subPlan.schema()
    # groupSchema: self.groupSchema;
    # aggSchema  : self.aggSchema;
    outputSchema = self.schema()
    
    if self.running:
        if set(locals().keys()).isdisjoint(set(inputSchema.fields)):
        
          for inputTuple in page:
              
            # Execute the projection expressions.
            inputTupleData = inputSchema.unpack( inputTuple );
            groupKey       = self.groupExpr( inputTupleData );
            if groupKey in self.grouping:
              tmpStore                = self.grouping[groupKey];
              self.grouping[groupKey] = [ self.aggExprs[i][1](tmpStore[i], inputTupleData) for i in range(0, len(self.aggExprs)) ]; 
                
            else:
              if not(sys.getsizeof(self.grouping) > self.memoryLimit):
                self.grouping[groupKey] = [ agg(init, inputTupleData) for (init, agg, _) in self.aggExprs ]; 
              else:
                self.putInPartition( groupKey, inputTuple ); 

        else:
            raise ValueError("Overlapping variables detected with operator schema")

    else:
        # We currently don't support not aggregatable value.
        raise NotImplementedError;
    
  # Set-at-a-time operator processing
  def processAllPages(self):
      
    if self.inputIterator is None:
      self.inputIterator = iter(self.subPlan)
    
    # initialize the grouping hash table.
    self.grouping = dict();

    # Process all pages from the child operator.
    try:
      for (pageId, page) in self.inputIterator:
        self.processInputPage(pageId, page)

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]
      
      # release memory of self.grouping before handling the tmp file data   
      self.outputGrouping( self.grouping );
      self.grouping.clear();
      gc.collect();
      
      if len(self.tmpFiles) > 0:
        for (k, v) in self.tmpFiles.items():
          pageIterator = v.pages();
          for (pageId, page) in pageIterator:
            self.processInputPage(pageId, page)
            
            # No need to track anything but the last output page when in batch mode.
            if self.outputPages:
              self.outputPages = [self.outputPages[-1]]
              
            self.outputGrouping( self.grouping );
            self.grouping.clear();
            gc.collect();
          # clean up all the tmp File
          self.storage.removeRelation(k);
          
    
    # To support pipelined operation, processInputPage may raise a
    # StopIteration exception during its work. We catch this and ignore in batch mode.
    except StopIteration:
      pass

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())


  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"

  def cost(self):
    return self.selectivity() * self.subPlan.cost()

  # This should be computed based on statistics collection.
  # For now, we simply return a constant.
  def selectivity(self):
    return 1.0
  
  # Some helper functions:
  def isAggExprRunning(self):
    if self.aggExprs:
      result = False;
      for aggExpr in self.aggExprs:
          agg     = aggExpr[ 1 ];
          running = agg( aggExpr[0], self.subSchema.default() );
          if ( isinstance(running, collections.Iterable) ):
            result = False;
          else:
            result = True;
      return result;
    else:
      raise RuntimeError("No aggExprs define for this operator");
  
  def sizeOfGroupingHash(self):
    if self.grouping:
      if self.running:
        return len(self.grouping);
      else:   
        return reduce(lambda x, y: x + y, (lambda dic : [len(v) for (k,v) in dic.items()])(self.grouping));
         
  def isGroupExprResultHashable(self):
    if not(isinstance( self.groupExpr(self.subSchema.default()), collections.Hashable) ):
      if isinstance(self.groupExpr(self.subSchema.default()), collections.Iterable):
        self.groupExprTmp = lambda e : tuple( self.groupExpr(e) );
        self.groupExpr    = self.groupExprTmp;
      else:
        raise ValueError("Please note that your grouping function should result in primitive hashable types or iterable collections");
    
  def outputGrouping(self, group):
      
    outputSchema = self.outputSchema;
    sizeOfAgg    = len(self.aggSchema.fields);
    
    for (k,v) in group.items():
      output = [];
      if isinstance(k, tuple):
        output = [ele for ele in k];
      else:
        output = [ k ];
    
      # processing v with final expr:
      tmpSt = v;
      v = [ self.aggExprs[i][2](tmpSt[i]) for i in range(0, sizeOfAgg)]
      output.extend(v);
      outputTuple = outputSchema.pack( output );
      self.emitOutputTuple(outputTuple);
    
    del group;

  def putInPartition(self, groupKey, tupleData):

    relIdTmp = self.relationId() + str( self.groupHashFn( groupKey ) ) + "GroupByTmp";
    
    if not(self.storage.hasRelation(relIdTmp)):
      self.storage.createRelation(relIdTmp, self.subPlan.schema());
      tempFile = self.storage.fileMgr.relationFile(relIdTmp)[1];
      self.tmpFiles[ relIdTmp ] = tempFile;
      tempFile.insertTuple( tupleData );
      
    else:
      self.tmpFiles[ relIdTmp ].insertTuple( tupleData );
    
      
      