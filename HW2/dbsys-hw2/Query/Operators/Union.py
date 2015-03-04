from Catalog.Schema import DBSchema
from Query.Operator import Operator
import itertools

class Union(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)
    self.lhsPlan  = lhsPlan
    self.rhsPlan  = rhsPlan
    self.validateSchema()

  # Unions must have equivalent schemas on their left and right inputs.
  def validateSchema(self):
    if self.lhsPlan.schema().match(self.rhsPlan.schema()):
      schemaName       = self.operatorType() + str(self.id())
      schemaFields     = self.lhsPlan.schema().schema()
      self.unionSchema = DBSchema(schemaName, schemaFields)
    else:
      raise ValueError("Union operator type error, mismatched input schemas")

  
  # Returns the output schema of this operator
  def schema(self):
    return self.unionSchema

  def inputSchemas(self):
    return [self.lhsPlan.schema(), self.rhsPlan.schema()]

  def operatorType(self):
    return "UnionAll"

  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for union operator.
  # The iterator must be set up to deal with input iterators and handle both pipelined and
  # non-pipelined cases
  def __iter__(self):
    self.initializeOutput();
    self.inputIterator = itertools.chain(self.lhsPlan, self.rhsPlan);
    self.lhsPlanLen = len(list(iter(self.lhsPlan)));
    self.itercounter = 0;
    self.inputFinished = False;

    if not self.pipelined:
      self.outputIterator = self.processAllPages();

    return self;

  # Method used for iteration, doing work in the process. Handle pipelined and non-pipelined cases
  def __next__(self):
    if self.pipelined:
      while not(self.inputFinished or self.isOutputPageReady()):
        try:
          pageId, page = next(self.inputIterator);
          self.processInputPage(pageId, page);
        except StopIteration:
          self.inputFinished = True;
          
      return self.outputPage();
    
    else:
      return next(self.outputIterator);


  # Page processing and control methods

  # Page-at-a-time operator processing
  # For union all, this copies over the input tuple to the output
  def processInputPage(self, pageId, page):
    inputSchemaL = self.inputSchemas()[0];
    inputSchemaR = self.inputSchemas()[1];
    outputSchema = self.schema();
    
    if (self.itercounter < self.lhsPlanLen):
      for inputTuple in page:
        outputTuple = inputTuple;
        self.emitOutputTuple(outputTuple);
    else:
      for inputTuple in page:
        inputR = self.loadSchema(inputSchemaR, inputTuple);
        outputTuple = outputSchema.pack([inputR[i] for i in outputSchema.fields])
        self.emitOutputTuple(outputTuple);
    
    self.itercounter += 1;
    
  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.inputIterator is None:
      self.inputIterator = itertools.chain(self.lhsPlan, self.rhsPlan);

    # Process all pages from the child operator.
    try:
      for (pageId, page) in self.inputIterator:
        self.processInputPage(pageId, page);

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]];

    # To support pipelined operation, processInputPage may raise a
    # StopIteration exception during its work. We catch this and ignore in batch mode.
    except StopIteration:
      pass;

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId());


  # Plan and statistics information

  # Returns a single line description of the operator.
  def cost(self):
    return self.selectivity() * sum(map(lambda x: x.cost(), self.inputs()))

  # Union all operators pass along every tuple that they consume as input.
  def selectivity(self):
    return 1.0

