#######################################################
# This PaxPage.py file implements PAX

import functools, math, struct
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import Types, DBSchema
from Storage.Page import PageHeader, Page
from Storage.SlottedPage import SlottedPageHeader, SlottedPage

# This is the header class of PAX page.
# The overall structure and field is similar to SlottedPage, so it inherits SlottedPageHeader

class PaxPageHeader(SlottedPageHeader):

    '''
    A PAX page header implementation. The PAX header should pack as
    (numSlots, nextSlot, numFields, [size for each field], slotbuffer)
    
    The followings are doctests:
    
    >>> import io
    >>> buffer = io.BytesIO(bytes(4096))
    >>> ph     = PaxPageHeader(buffer=buffer.getbuffer(), schemaSizes=[4,8,2,2])
    >>> ph2    = PaxPageHeader.unpack(buffer.getbuffer())

    ## Dirty bit tests
    >>> ph.isDirty()
    False
    >>> ph.setDirty(True)
    >>> ph.isDirty()
    True
    >>> ph.setDirty(False)
    >>> ph.isDirty()
    False

    ## Tuple count tests
    >>> ph.hasFreeTuple()
    True

    # First tuple allocated should be at the first slot.
    # Notice this is a slot index, not an offset as with contiguous pages.
    >>> ph.nextFreeTuple() == 0
    True

    >>> ph.numTuples()
    1

    >>> tuplesToTest = 10
    >>> [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  
    >>> ph.numTuples() == tuplesToTest+1
    True

    >>> ph.hasFreeTuple()
    True

    # Check space utilization
    >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
    True

    >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
    True

    >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

    # Fill the page.
    >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
    [11, 12, ...]

    >>> ph.hasFreeTuple()
    False

    # No value is returned when trying to exceed the page capacity.
    >>> ph.nextFreeTuple() == None
    True
  
    >>> ph.freeSpace() < ph.tupleSize
    True
    
    # Testing offset calculations:
    >>> ph.offsets[0] == ph.headerSize();
    True
    
    >>> ph.offsets[1] == ph.headerSize() + ph.numSlots * 4;
    True
    
    >>> ph.offsets[2] == ph.headerSize() + ph.numSlots * 12;
    True
    >>> ph.offsetOfSlot( 9 )[0] == ph.headerSize() + 9 * 4;
    True
    '''
    
    def __init__(self, **kwargs):

        buffer     = kwargs.get("buffer", None)
        self.flags = kwargs.get("flags", b'\x00')
        recons     = kwargs.get("reconstruct", False);
        
        if buffer:
            
            self.schemaSizes   = kwargs.get("schemaSizes", []);
            self.tupleSize     = functools.reduce(lambda x, y: x+y, self.schemaSizes);
            self.pageCapacity  = len(buffer);
            self.numSlots      = math.floor( ( self.pageCapacity - 2 * ( 3 + len(self.schemaSizes) ) )  / float( self.tupleSize + 0.125 ) )  # every bit takes 1/8 byte.
      
            # We keep an array of slots which should be updated. 
            self.slots            = [0 for x in range(0, self.numSlots)];
      
            # We implements a queue by list for availableSlots.
            self.availableSlots   = [];
            self.slotBufferLength = math.ceil( float(self.numSlots) / 8 );
            self.nextSlot         = 0;
            self.slotUsed         = [];
            self.numUsed          = 0;
      
            # initializing availableSlots
            for index in range(0, self.numSlots):
               self.availableSlots.append(index);
            
            # initializing offsets for each fields
            self.initializeOffsets();
          
            # We write to buffer
            if not recons:
               buffer[0:self.headerSize()] = self.pack();

        else:
            raise ValueError("No backing buffer supplied for SlottedPageHeader")

    def __eq__(self, other):
        return (    self.flags == other.flags
                and self.schemaSizes == other.schemaSizes
                and self.pageCapacity == other.pageCapacity
                and self.nextSlot  == other.nextSlot
                and self.slots     == other.slots )

    def __hash__(self):
        return hash((self.flags, self.tupleSize, self.pageCapacity, self.nextSlot, self.hashSlotsHelper()))

    def headerSize(self):
        return 2 * ( 3 + len(self.schemaSizes) ) + self.slotBufferLength; 
    
    def initializeOffsets(self):
        
        tmpCount     = self.headerSize();
        self.offsets = [ tmpCount ];
        
        for i in range(0, len(self.schemaSizes) - 1):
            tmpCount              += self.schemaSizes[i] * self.numSlots;
            self.offsets.append( tmpCount );

  # Flag operations.
    def flag(self, mask):
        return (ord(self.flags) & mask) > 0

    def setFlag(self, mask, set):
        if set:
            self.flags = bytes([ord(self.flags) | mask])
        else:
            self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
    def isDirty(self):
        return self.flag(PageHeader.dirtyMask)

    def setDirty(self, dirty):
        self.setFlag(PageHeader.dirtyMask, dirty)
        
    def numTuples(self):
    # if one tuple is occupied, one bit in the bitmaps is marked as 1.
        return super(PaxPageHeader, self).numTuples();
    
    # Returns the space available in the page associated with this header.
    def freeSpace(self):
        return super(PaxPageHeader, self).freeSpace();

    # Returns the space used in the page associated with this header.
    def usedSpace(self):
        return super(PaxPageHeader, self).usedSpace();
    
    # Slot operations.
    def offsetOfSlot(self, slotIndex):
        return list(map(lambda baseOffset, size: baseOffset + slotIndex * size, self.offsets, self.schemaSizes ));

    def resetSlot(self, slotIndex):
        return super(PaxPageHeader, self).resetSlot( slotIndex );

    def usedSlots(self):
        return super(PaxPageHeader, self).usedSlots();
    
    def hasFreeTuple(self):
        return (self.numUsed < self.numSlots);
    
    def nextFreeTuple(self):
        return super(PaxPageHeader, self).nextFreeTuple();
    
    def pack(self):
    
        packedHeader = struct.pack("HHH", self.numSlots, self.nextSlot, len(self.schemaSizes));
        packedHeader += struct.pack('H' * len(self.schemaSizes), *(self.schemaSizes));
    
        # Next, we packed our slot buffer to bits
        bitSlot = 0
        count8  = 0
    
        for i in range(0, self.numSlots):
            if i == 0:
                bitSlot = self.slots[0];
            
            elif self.mod8(i) and i != 0:
                packedHeader += Struct("B").pack(bitSlot);
                bitSlot = self.slots[i];
                count8  = 0;
            else:
                count8  += 1;
                bitSlot |= ( self.slots[i] << count8 );
            
            if i == (self.numSlots - 1):
                packedHeader += Struct("B").pack(bitSlot);
    
        return packedHeader;

    # Create a slotted page header instance from a binary representation held in the given buffer.
    @classmethod
    def unpack(cls, buffer):

        values    = Struct("HHH").unpack_from(buffer);
        numSlots  = values[0];
        nextSlot  = values[1];
        numSize   = values[2];
        
        schemaSizes = list( Struct('H' * numSize).unpack_from(buffer, offset=6) );
        
        ph        = cls(buffer=buffer, schemaSizes=schemaSizes, reconstruct=True);
        ph.numSlots = numSlots;
        ph.nextSlot = nextSlot;
    
        count     = 0;
    
        # reconstructing slots
        for i in range( 2 * (3 + numSize), ph.headerSize()):
        
            count8 = 0;
            q = buffer[i];
            while count8 < 8:
                ph.slots[ count ] = q & 1;
                q      >>= 1;
                count8 +=  1;
                count  +=  1; 
        
                if count >= ph.numSlots:
                    break;
        
            continue;
    
        # reconstructing available slots;
        ph.availableSlots = [];
        for i in range(0, ph.numSlots):
            if ph.slots[i] == 0:
                ph.availableSlots.append(i);
            if ph.slots[i] == 1:
                ph.slotUsed.append(i);
    
        ph.numUsed = len(ph.slotUsed);  
        return ph;

    # We implement a function that can return whether index mod 8 = 0?
    def mod8(self, index):
        return super(PaxPageHeader, self).mod8( index );
   
    def hashSlotsHelper(self):
        return super(PaxPageHeader, self).hashSlotsHelper();
    
    
######################################################
# This is an implementation of PAX page
# This page inherits from SlottedPage. 
# Note that page is a low-level storage unit so that 
# use contractual programming but not defensive.
#
class PaxPage(SlottedPage):
  """
  A PAX page implementation.

  PAX pages use the PaxPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = PaxPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  >>> p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  >>> tId.tupleIndex
  0

  # Retrieve the previous tuple
  >>> e2 = schema.unpack(p.getTuple(tId))
  >>> e2
  employee(id=1, age=25)

  # Update the tuple.
  >>> e1 = schema.instantiate(1,28)
  >>> p.putTuple(tId, schema.pack(e1))

  # Retrieve the update
  >>> e3 = schema.unpack(p.getTuple(tId))
  >>> e3
  employee(id=1, age=28)

  # Compare tuples
  >>> e1 == e3
  True

  >>> e2 == e3
  False

  # Check number of tuples in page
  >>> p.header.numTuples() == 1
  True

  # Add some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples()
  11

  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0)
  >>> sizeBeforeClear = p.header.usedSpace()  
  >>> p.clearTuple(tId)
  
  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  >>> [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  >>> sizeBeforeRemove = p.header.usedSpace()
  >>> p.deleteTuple(tId)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]
  
  # Check that the page's slots have tracked the deletion.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True
  
  # Testing getTupleField() and iterator
  >>> e2 = schema.instantiate(1,26)
  >>> tId1 = p.insertTuple(schema.pack(e2))
  >>> import struct
  >>> struct.unpack('i', p.getTupleField(tId1, 1))[0]
  26
  >>> [age for age in p.column(1)]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 26]

  """   
  headerClass = PaxPageHeader
  
  def __init__(self, **kwargs):

    buffer = kwargs.get("buffer", None)
    if buffer:
      BytesIO.__init__(self, buffer)
      self.pageId = kwargs.get("pageId", None)
      header      = kwargs.get("header", None)
      schema      = kwargs.get("schema", None)

      if self.pageId and header:
        self.header = header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")

    else:
      raise ValueError("No backing buffer provided to page constructor.")
  
  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      schemaSizes = [Struct(str).size for str in [Types.formatType(x) for x in schema.types]];
      self.types  = [str for str in [Types.formatType(x) for x in schema.types]];
      return PaxPageHeader(buffer=self.getbuffer(), schemaSizes=schemaSizes);
    else:
      raise ValueError("No schema provided when constructing a slotted page.")
  
  # Tuple iterator.
  def __iter__(self):
      
    self.iterTupleIdx = 0;
    return self

  def __next__(self):
      
    if self.iterTupleIdx < len(self.header.slotUsed):
        t = self.getTuple(TupleId(self.pageId, self.header.slotUsed[self.iterTupleIdx]));
        self.iterTupleIdx += 1;
        return t;
    else:
      raise StopIteration
  
  # Tuple accessor methods

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):
      
    slotIndex = tupleId.tupleIndex;
    start     = self.header.offsetOfSlot(slotIndex);
    t         = b'';
    for i in range(0, len(self.header.schemaSizes)):
        t += self.getvalue()[start[i] : (start[i] + self.header.schemaSizes[i])]
    return t;

  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
      
    slotIndex = tupleId.tupleIndex;
    if slotIndex in self.header.slotUsed:
        start     = self.header.offsetOfSlot(slotIndex);
        tupleSt   = 0;
        for i in range(0, len(self.header.schemaSizes)):
            self.getbuffer()[start[i] : (start[i] + self.header.schemaSizes[i])] = \
            tupleData[ tupleSt : (tupleSt + self.header.schemaSizes[i]) ];
            tupleSt += self.header.schemaSizes[i];
        self.header.setDirty( True );
    else:
        raise ValueError("tupleId is not valid. It is an empty one");
    
    
  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
      
    slotIndex = tupleId.tupleIndex;
    start     = self.header.offsetOfSlot(slotIndex);
    for i in range(0, len(self.header.schemaSizes)):
        self.getbuffer()[start[i] : (start[i] + self.header.schemaSizes[i])] = \
        bytearray(b'\x00' * self.header.schemaSizes[i]);
    self.header.setDirty( True );
    
  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
      
    if self.header.hasFreeTuple():
        slotIndex = self.header.nextFreeTuple();
        start     = self.header.offsetOfSlot(slotIndex);
        tupleSt   = 0;
        
        for i in range(0, len(self.header.schemaSizes)):
            self.getbuffer()[start[i] : (start[i] + self.header.schemaSizes[i])] = \
            tupleData[ tupleSt : (tupleSt + self.header.schemaSizes[i]) ];
            tupleSt += self.header.schemaSizes[i];
            
        self.header.setDirty( True );
        return TupleId(self.pageId, slotIndex);
    else:
        raise ValueError("This page is full!");
    
  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    return super(PaxPage, self).deleteTuple( tupleId );

  def getTupleField(self, tupleId, fieldPosition):
    
    slotIndex = tupleId.tupleIndex;
    start     = self.header.offsetOfSlot(slotIndex)[ fieldPosition ];
    t         = self.getvalue()[start : (start + self.header.schemaSizes[ fieldPosition ])];
    return t;

  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):
    return super(PaxPage, self).pack();

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    return super(PaxPage, cls).unpack(pageId, buffer); 

  # This function is an iterator over column data.
  def column(self, fieldPosition):
    return self.ColumnDataIterator( self, fieldPosition );

  # This function is a helper function for iterator
  def tupleId(self, tupleIndex):
    return TupleId(self.pageId, tupleIndex);

  # inner class for Column Data iterator
  class ColumnDataIterator:
      
    def __init__(self, paxPage, fieldPosition):
      self.currentTupleIdx = 0;
      self.paxPage    = paxPage;
      self.fieldPos   = fieldPosition;

    def __iter__(self):
      return self

    def __next__(self):
      if self.currentTupleIdx < self.paxPage.header.numUsed:
          tId = self.paxPage.tupleId( self.paxPage.header.slotUsed[ self.currentTupleIdx ]);
          self.currentTupleIdx += 1;
          return Struct( self.paxPage.types[ self.fieldPos ] ).unpack(self.paxPage.getTupleField(tId, self.fieldPos))[0];
      else:
          raise StopIteration
       
    
if __name__ == "__main__":
    import doctest
    doctest.testmod()  
