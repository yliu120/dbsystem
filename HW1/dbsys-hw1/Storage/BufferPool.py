import io, math, struct

from collections import OrderedDict
from struct      import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Check initial buffer pool size
  >>> len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  # Default to a 10 MB buffer pool.
  defaultPoolSize = 10 * (1 << 20)

  # Buffer pool constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # pageSize       : the page size to be used with this buffer pool
  # poolSize       : the size of the buffer pool
  def __init__(self, **kwargs):
    self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)
    self.pool         = io.BytesIO(b'\x00' * self.poolSize)

    ####################################################################################
    # DESIGN QUESTION: what other data structures do we need to keep in the buffer pool?
    
    # Buffer Pool Frames
    # Dictionary : offset -> (pId, pin number)
    # ? We can keep a backward mapping as well
    self.frames       = {x : None for x in range(0, self.poolSize, self.pageSize)};
    self.backward     = dict();
    
    # Use a queue to store freeList
    self.freeList     = self.frames.keys();
    
    # Buffer Pool replacement queue ( Least Recently Used )
    self.replaceQ     = OrderedDict();


  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr

  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize / self.pageSize)

  def numFreePages(self):
    return len( self.freeList ); 

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()


  # Buffer pool operations

  def hasPage(self, pageId):
    return (pageId in self.backward);
  
  def getPage(self, pageId):
      
    if self.hasPage(pageId):
       # Here we only have one requestor, no need to pin the page       
       # update replacement ordered dictionary
       self.replaceQ.move_to_end(pageId, last = True);
       
       # return page object to requestor
       return self.frames[ self.backward(pageId) ];
    
    else:
       # Cache miss
       if self.freeList is []:
          self.evictPage();
          # Here we are not thinking of concurrency problem.
          # Then by the algorithm, now we should have 1 element in the freeList
          # read the page to pool buffer:
          buffer = self.pool.getbuffer()[ self.freeList[0] : (self.freeList[0] + self.pageSize) ];
          page   = self.fileMgr.readPage(pageId, buffer);
          # add page to our datastructure
          self.frames  [ self.freeList[0] ] = page;
          self.backward[ pageId ]           = self.freeList[0];
          self.replaceQ.update({pageId : self.freelist[0]});
          self.freelist.pop(0);
       else:
          offset = self.freeList.pop(0);
          buffer = self.pool.getbuffer()[ offset : (offset + self.pageSize) ];
          page   = self.fileMgr.readPage(pageId, buffer);
          # Maintain datastructures
          self.frames  [ offset ] = page;
          self.backward[ pageId ] = offset;
          self.replaceQ.update({pageId : offset});
       
           
  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    
    self.freeList.append( self.backward[pageId] );
    self.backward.pop(pageId, None);
    self.replaceQ.pop(pageId);

  def flushPage(self, pageId):
    self.fileMgr.writePage(pageId);

  # Evict using LRU policy. 
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
      
    (pageId, offset) = self.replaceQ.popitem(last=False);
    # Note that we don't need to check the pin of pageId here
    self.freeList.append( offset );
    # We write the evicted page back to the disk;
    self.flushPage(pageId);
    # Maintain our data structure
    self.backward.pop(pageId, None);
    

if __name__ == "__main__":
    import doctest
    doctest.testmod()
