import os;

class DBFileSystemGC:
  
  
  defaultDBPath = './data';
  # close all the files in that directory.
  @staticmethod
  def list_files(path):
    # returns a list of names (with extension, without full path) of all files 
    # in folder path
    files = []
    for name in os.listdir(path):
      if os.path.isfile(os.path.join(path, name)):
        files.append(name)
    return files 
  
  @staticmethod
  def gc(opMarker=None, db=None):
    fileNames = DBFileSystemGC.list_files(defaultDBPath);

    for file in fileNames:
      fName = defaultDBPath + '/' + file;
      f = open(fName, 'r');
      f.close(); 
    
    if db == None:
      db = Database(dataDir=defaultDBPath);
      
    if opMarker == None:
      opMarker = "";

    tmpRel = list( db.storage.fileMgr.relations() );
    for relKey in tmpRel:
      if relKey.startswith('tmp') and relKey.endswith(opMarker):
        db.storage.fileMgr.removeRelation( relKey );
  
