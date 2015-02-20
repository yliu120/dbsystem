#######################################################
# This PaxPage.py file implements PAX

import functools, math, struct
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page
from Storage.SlottedPage import SlottedPageHeader, SlottedPage

# This is the header class of PAX page.
# The overall structure and field is similar to SlottedPage, so it inherits SlottedPageHeader

class PaxPageHeader(SlottedPageHeader):

    '''
    A PAX page header implementation. The PAX header should pack as
    (numSlots, nextSlot, numFields, [size for each field], slotbuffer)
    
    The followings are doctests:
    '''
    
    def __init__(self, **kwargs):

        buffer     = kwargs.get("buffer", None)
        self.flags = kwargs.get("flags", b'\x00')
        recons     = kwargs.get("reconstruct", False);
        
        if buffer:
            
            self.schemaSizes   = kwargs.get("schemaSizes", []);
            tupleSize          = functools.reduce(lambda x, y: x+y, self.schemaSizes);
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
        packedHeader += struct.pack("sH" % len(self.schemaSizes), *(self.schemaSizes));
    
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
        
        schemaSizes = list( Struct("sH" % numSize).unpack_from(buffer, offset=6) );
        
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
    
    
    
    
