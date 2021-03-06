import io, math, os, os.path, pickle, struct
from struct import Struct
from collections import OrderedDict

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema
from Storage.Page        import PageHeader, Page
from Storage.SlottedPage import SlottedPageHeader, SlottedPage
from Storage.PaxPage     import PaxPageHeader, PaxPage  


class FileHeader:
  """
  A file header class, containing a page size and a schema for the data
  entries stored in the file.

  Our file header object also keeps its own binary representation per instance
  rather than at the class level, since each file may have a variable length schema.
  The binary representation is a struct, with three components in its format string:
  i.   header length
  ii.  page size
  iii. a JSON-serialized schema (from DBSchema.packSchema)

  >>> schema = DBSchema('employee', [('id', 'int'), ('dob', 'char(10)'), ('salary', 'int')])
  >>> fh = FileHeader(pageSize=io.DEFAULT_BUFFER_SIZE, pageClass=SlottedPage, schema=schema)
  >>> b = fh.pack()
  >>> fh2 = FileHeader.unpack(b)
  >>> fh.pageSize == fh2.pageSize
  True

  >>> fh.schema.schema() == fh2.schema.schema()
  True

  ## Test the file header's ability to be written to, and read from a Python file object.
  >>> f1 = open('test.header', 'wb')
  >>> fh.toFile(f1)
  >>> f1.flush(); f1.close()

  >>> f2 = open('test.header', 'r+b')
  >>> fh3 = FileHeader.fromFile(f2)
  >>> fh.pageSize == fh3.pageSize \
      and fh.pageClass == fh3.pageClass \
      and fh.schema.schema() == fh3.schema.schema()
  True

  >>> os.remove('test.header')
  """

  def __init__(self, **kwargs):
    other = kwargs.get("other", None) 
    if other:
      self.fromOther(other)

    else:
      pageSize    = kwargs.get("pageSize", None)
      pageClass   = kwargs.get("pageClass", None)
      schema      = kwargs.get("schema", None)

      if pageSize and pageClass and schema:
        pageClassLen   = len(pickle.dumps(pageClass))
        schemaDescLen  = len(schema.packSchema())
        self.binrepr   = Struct("HHHH"+str(pageClassLen)+"s"+str(schemaDescLen)+"s")
        self.size      = self.binrepr.size
        self.pageSize  = pageSize
        self.pageClass = pageClass
        self.schema    = schema

      else:
        raise ValueError("Invalid file header constructor arguments")

  def fromOther(self, other):
    self.binrepr   = other.binrepr
    self.size      = other.size
    self.pageSize  = other.pageSize
    self.pageClass = other.pageClass
    self.schema    = other.schema

  def pack(self):
    if self.binrepr and self.pageSize and self.schema:
      packedPageClass = pickle.dumps(self.pageClass)
      packedSchema    = self.schema.packSchema()
      return self.binrepr.pack(self.size, self.pageSize, \
              len(packedPageClass), len(packedSchema), \
              packedPageClass, packedSchema)

  @classmethod
  def binrepr(cls, buffer):
    lenStruct = Struct("HHHH")
    (headerLen, _, pageClassLen, schemaDescLen) = lenStruct.unpack_from(buffer)
    if headerLen > 0 and pageClassLen > 0 and schemaDescLen > 0:
      return Struct("HHHH"+str(pageClassLen)+"s"+str(schemaDescLen)+"s")
    else:
      raise ValueError("Invalid header length read from storage file header")

  @classmethod
  def unpack(cls, buffer):
    brepr  = cls.binrepr(buffer)
    values = brepr.unpack_from(buffer)
    if len(values) == 6:
      pageClass = pickle.loads(values[4])
      schema    = DBSchema.unpackSchema(values[5])
      return FileHeader(pageSize=values[1], pageClass=pageClass, schema=schema)

  def toFile(self, f):
    pos = f.tell()
    if pos == 0:
      f.write(self.pack())
    else:
      raise ValueError("Cannot write file header, file positioned beyond its start.")

  @classmethod
  def fromFile(cls, f):
    pos = f.tell()
    if pos == 0:
      lenStruct = Struct("H")
      headerLen = lenStruct.unpack_from(f.peek(lenStruct.size))[0]
      if headerLen > 0:
        buffer = f.read(headerLen)
        return FileHeader.unpack(buffer)
      else:
        raise ValueError("Invalid header length read from storage file header")
    else:
      raise ValueError("Cannot read file header, file positioned beyond its start.")



class StorageFile:
  """
  A storage file implementation, as a base class for all database files.

  All storage files have a file identifier, a file path, a file header and a handle
  to a file object as metadata.

  This implementation supports a readPage() and writePage() method, enabling I/O
  for specific pages to the backing file. Allocation of new pages is handled by the
  underlying file system (i.e. simply write the desired page, and the file system 
  will grow the backing file by the desired amount).

  Storage files may also serialize their metadata using the pack() and unpack(),
  allowing their metadata to be written to disk when persisting the database catalog.

  >>> import shutil, Storage.BufferPool, Storage.FileManager
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = Storage.BufferPool.BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Create a relation for the given schema
  >>> fm.createRelation(schema.name, schema)
  
  # Below 'f' is a StorageFile object returned by the FileManager
  >>> (fId, f) = fm.relationFile(schema.name)

  # Check initial file status
  >>> f.numPages() == 0
  True

  # There should be a valid free page data structure in the file.
  >>> f.freePages is not None
  True

  # The first available page should be at page offset 0.
  >>> f.availablePage().pageIndex
  0

  # Create a pair of pages.
  >>> pId  = PageId(fId, 0)
  >>> pId1 = PageId(fId, 1)
  >>> p    = SlottedPage(pageId=pId,  buffer=bytes(f.pageSize()), schema=schema)
  >>> p1   = SlottedPage(pageId=pId1, buffer=bytes(f.pageSize()), schema=schema)

  # Populate pages
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  >>> for tup in [schema.pack(schema.instantiate(i, i+20)) for i in range(10, 20)]:
  ...    _ = p1.insertTuple(tup)
  ...

  # Write out pages and sync to disk.
  >>> f.writePage(p)
  >>> f.writePage(p1)
  
  # Testing read page header function
  >>> p.header.usedSpace()
  80
  
  >>> h1 = f.readPageHeader( pId );
  >>> h2 = f.readPageHeader( pId1 );
  
  >>> h1.usedSpace()
  80
  >>> h2.usedSpace()
  80
  
  >>> f.flush()
  
  # Check the number of pages, and the file size.
  >>> f.numPages() == 2
  True

  >>> f.size() == (f.headerSize() + f.pageSize() * 2)
  True

  # Read pages in reverse order testing offset and page index.
  >>> pageBuffer = bytearray(f.pageSize())
  >>> pIn1 = f.readPage(pId1, pageBuffer)
  >>> pIn1.pageId == pId1
  True

  >>> f.pageOffset(pIn1.pageId) == f.header.size + f.pageSize()
  True
  
  >>> pIn = f.readPage(pId, pageBuffer)
  >>> pIn.pageId == pId
  True

  >>> f.pageOffset(pIn.pageId) == f.header.size
  True

  # Test page header iterator
  >>> [p[1].usedSpace() for p in f.headers()]
  [80, 80]

  # Test page iterator
  >>> [p[1].pageId.pageIndex for p in f.pages()]
  [0, 1]

  # Test tuple iterator
  >>> [schema.unpack(tup).id for tup in f.tuples()] == list(range(20))
  True

  # Check buffer pool utilization
  >>> (bp.numPages() - bp.numFreePages()) == 2
  True

  ## Clean up the doctest
  >>> shutil.rmtree(Storage.FileManager.FileManager.defaultDataDir)
  """

  # Change this to the Page class if you want contiguous page storage in the file.
  defaultPageClass = SlottedPage

  # StorageFile constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # bufferPool   : a buffer pool instance.
  # fileId       : a FileId instance identifying this file.
  # filePath     : a PageHeader instance.
  # mode         : the file open mode. Can be one of 'create', 'update', 'truncate'.
  # Also, any keyword arguments needed to construct a FileHeader.
  def __init__(self, **kwargs):
    self.bufferPool = kwargs.get("bufferPool", None)
    if self.bufferPool is None:
      raise ValueError("No buffer pool found when initializing a storage file")

    pageSize       = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    pageClass      = kwargs.get("pageClass", StorageFile.defaultPageClass)
    schema         = kwargs.get("schema", None)
    mode           = kwargs.get("mode", None)

    self.fileId    = kwargs.get("fileId", None)
    self.filePath  = kwargs.get("filePath", None)

    ######################################################################################
    # DESIGN QUESTION: how do you initialize these?
    # 1) The file should be opened depending on the desired mode of operation.
    # ANS: Python file operations:
    # if you want to just append to an existing file, use the append
    # mode:

    # f = open(your_file, 'a')
    # if you want to both read *and* write to your existing file, use
    # update mode:

    # f = open(your_file, 'r+')

    # the '+' means open the file for reading and writing, i.e.
    # update.  ('w+' means delete the existing file and open a new
    # one for read and write so be careful!) anyway, once you open
    # the file, you can use f.seek() and f.tell() to navigate within
    # the file.  if you are going to read and write to your file,
    # make sure you explicitly f.flush() between change of operations.
    
    # 2) The file header may come from the file contents (i.e., if the file already exists),
    # otherwise it should be created from scratch.
    
    if mode == "create" :
        self.file   = open( self.filePath, 'b+w' );
        self.header = FileHeader(pageSize=pageSize, pageClass=pageClass, schema=schema);
        self.file.write( self.header.pack() );
        self.flush();
    elif mode == "update" or mode == "truncate" :
        self.file   = open( self.filePath, 'br+' );
        self.header = FileHeader.fromFile( self.file );
    else:
        raise ValueError("Storage.StorageFile: Constructor -> mode input invalid.")

    ######################################################################################
    # DESIGN QUESTION: what data structure do you use to keep track of the free pages?
    # ANS            : QUEUE - need to be reconstructed by unpack()
    self.freePages = OrderedDict();
    self.pageNum  = 0;

  # File control
  def flush(self):
    self.file.flush()

  def close(self):
    if not self.file.closed:
      self.file.close()

  # Storage file helpers
  def pageId(self, pageIndex):
    return PageId(self.fileId, pageIndex)

  def schema(self):
    return self.header.schema

  def pageSize(self):
    return self.header.pageSize

  def pageClass(self):
    return self.header.pageClass

  def size(self):
    return os.path.getsize(self.filePath)

  def headerSize(self):
    return self.header.size;

  def numPages(self):
    return self.pageNum;

  def checkPageClass(self, page):
    if not isinstance(page, self.pageClass()):
       raise("The page class in hand is not compatible with this file.")

  # Returns the offset in the file corresponding to the given page id.
  # Notice this assumes the header is written before the first page,
  # and is not part of the first page itself.
  def pageOffset(self, pageId):
    return self.headerSize() + self.pageSize() * pageId.pageIndex

  # Returns whether the given page id is valid for this file.
  def validPageId(self, pageId):
    return pageId.fileId == self.fileId and pageId.pageIndex < self.numPages()

  # Page header operations

  # Reads a page header from disk.
  def readPageHeader(self, pageId):
    
    # Set the file handle to the correct page 
    if self.validPageId( pageId ): 
        offset = self.pageOffset( pageId );
        self.file.seek( offset );
        buffer = io.BytesIO( self.file.read( self.pageSize() ) );
        return self.pageClass().headerClass.unpack( buffer.getbuffer() );
    else:
        raise ValueError("Storage.StorageFile -> pageId out of range. (pid: " + str(pageId.pageIndex) + ")" );

  # Writes a page header to disk.
  # The page must already exist, that is we cannot extend the file with only a page header.
  def writePageHeader(self, page):

    self.file.seek( self.pageOffset( page.pageId ) );
    self.file.write( page.header.pack() );
    self.flush();
    
  # Page operations
  # page : the page buffer
  def readPage(self, pageId, page):
    
    if self.validPageId(pageId):
        self.file.seek( self.pageOffset(pageId) );
        page = self.file.read( self.pageSize() );
        return self.pageClass().unpack( pageId, page );
    else:
        raise ValueError("StorageFile: Read Page error -> invalid pageId");

  def writePage(self, page):
    
    self.checkPageClass(page);
    
    pId = page.pageId;
    self.file.seek( self.pageOffset(pId) );
    self.file.write( page.pack() );
    self.flush();
    
    if not self.validPageId( pId ):
        self.pageNum += 1;
        # update our freepages data structure
        if page.header.hasFreeTuple():
           self.freePages[pId] = page;

  # Adds a new page to the file by writing past its end.
  def allocatePage(self):
    
    pId  = PageId( self.fileId, self.pageNum );
    page = self.pageClass()( pageId=pId, buffer=bytes(self.pageSize()), schema=self.schema() );
    self.file.write( page.pack() );
    # increment page num
    self.pageNum += 1;
    
    # update freepages
    self.freePages[pId] = page;

  # Returns the page id of the first page with available space.
  def availablePage(self):
    
    if not self.freePages:
        self.allocatePage();
    
    return list(self.freePages.items())[0][0];

  # Tuple operations

  # Inserts the given tuple to the first available page.
  def insertTuple(self, tupleData):
    
    avaPageID = self.availablePage();
    page      = self.freePages[ avaPageID ];
    
    tupleId   = page.insertTuple( tupleData );
    
    if not page.header.hasFreeTuple():
       self.freePages.popitem(last=False);

    self.writePage(page);
    return tupleId;
        
  # Removes the tuple by its id, tracking if the page is now free
  def deleteTuple(self, tupleId):
      
    pId  = tupleId.pageId;
    
    if pId in self.freePages:
        self.freePages[pId].deleteTuple( tupleId );
        self.writePage(page);
        return;
    
    else:
        page = self.readPage(pId, bytes( self.pageSize() ));
    
        pre  = page.header.hasFreeTuple();
    
        page.deleteTuple( tupleId );
        self.writePage(page);
    
        if (not pre) and page.header.hasFreeTuple():
            self.freePages[pId] = page;
            self.freePages.move_to_end(pId, last=False);
        else:
            return;

  # Updates the tuple by id
  def updateTuple(self, tupleId, tupleData):
    
    pId  = tupleId.pageId;
    
    if pId in self.freePages:
        self.freePages[pId].putTuple( tupleId, tupleData );
        return;
    
    else:
        page = self.readPage(pId, bytes( self.pageSize() ));
        page.putTuple(tupleId, tupleData);
    
    self.writePage(page);

  # Iterators
  # Page header iterator
  def headers(self):
    return self.FileHeaderIterator(self)
  
  # Page iterator, using the buffer pool
  def pages(self):
    return self.FilePageIterator(self)

  # Unbuffered page iterator.
  # Use with care, direct pages are not authoritative if the page is present in the buffer pool.
  def directPages(self):
    return self.FileDirectPageIterator(self)

  # Tuple iterator
  def tuples(self):
    return self.FileTupleIterator(self)


  # Iterator class implementations
  class FileHeaderIterator:
    def __init__(self, storageFile):
      self.currentPageIdx = 0
      self.storageFile    = storageFile

    def __iter__(self):
      return self

    def __next__(self):
      pId = self.storageFile.pageId(self.currentPageIdx)
      if self.storageFile.validPageId(pId):
        self.currentPageIdx += 1
        if self.storageFile.bufferPool.hasPage(pId):
          return (pId, self.storageFile.bufferPool.getPage(pId).header)
        else:
          return (pId, self.storageFile.readPageHeader(pId))
      else:
        raise StopIteration

  class FilePageIterator:
    def __init__(self, storageFile):
      self.currentPageIdx = 0
      self.storageFile    = storageFile

    def __iter__(self):
      return self

    def __next__(self):
      pId = self.storageFile.pageId(self.currentPageIdx)
      if self.storageFile.validPageId(pId):
        self.currentPageIdx += 1
        return (pId, self.storageFile.bufferPool.getPage(pId))
      else:
        raise StopIteration

  class FileDirectPageIterator:
    def __init__(self, storageFile):
      self.currentPageIdx = 0
      self.storageFile    = storageFile
      self.buffer         = bytearray(storageFile.pageSize())

    def __iter__(self):
      return self

    def __next__(self):
      pId = self.storageFile.pageId(self.currentPageIdx)
      if self.storageFile.validPageId(pId):
        self.currentPageIdx += 1
        return (pId, self.storageFile.readPage(pId, self.buffer))
      else:
        raise StopIteration

  class FileTupleIterator:
    def __init__(self, storageFile):
      self.storageFile     = storageFile
      self.pageIterator    = storageFile.pages()
      self.nextPage()

    def __iter__(self):
      return self

    def __next__(self):
      if self.pageIterator is not None:
        while self.tupleIterator is not None:
          try:
            return next(self.tupleIterator)
          except StopIteration:
            self.nextPage()
      
      if self.pageIterator is None:
        raise StopIteration

    def nextPage(self):
      try:
        self.currentPage   = next(self.pageIterator)[1]
      except StopIteration:
        self.pageIterator  = None
        self.tupleIterator = None
      else:
        self.tupleIterator = iter(self.currentPage)        


if __name__ == "__main__":
    import doctest
    doctest.testmod()
