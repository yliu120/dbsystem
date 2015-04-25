from Database import Database
from Catalog.Schema import DBSchema
import os;

# close all the files in that directory.
def list_files(path):
    # returns a list of names (with extension, without full path) of all files 
    # in folder path
    files = []
    for name in os.listdir(path):
        if os.path.isfile(os.path.join(path, name)):
            files.append(name)
    return files 
  
fileNames = list_files('./data');

for file in fileNames:
  fName = "./data/" + file;
  f = open(fName, 'r');
  f.close(); 
  
db = Database(dataDir='./data');

tmpRel = db.relations();
for relKey in tmpRel:
  db.removeRelation( relKey );
  
