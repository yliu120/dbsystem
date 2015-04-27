from DBFileSystemGC import DBFileSystemGC
DBFileSystemGC.gc()
import Database
db = Database.Database()
db.removeRelation('department');
db.removeRelation('employee');
db.removeRelation('project');
db.removeRelation('grant');
db.removeRelation('syn1');
db.removeRelation('syn2');
from DBFileSystemGC import DBFileSystemGC
DBFileSystemGC.gc()
db.close();

