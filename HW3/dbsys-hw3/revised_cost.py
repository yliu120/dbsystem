import math

class Join:
  def localCost(self, estimated):
    numInputs = sum(map(lambda x: x.cardinality(estimated), self.inputs()));
    l_inputPages = self.lhsPlan.tempFile.numPages();
    r_inputPages = self.rhsPlan.tempFile.numPages();
    l_tmpFileSize = self.lhsPlan.tempFile.size();
    if estimated:
      l_inputPages *= self.sampleFactor;
      r_inputPages *= self.sampleFactor;
      l_tmpFileSize *= self.sampleFactor;
    
    if (self.joinMethod == "nested-loops"):
      local_cost = l_inputPages + self.lhsPlan.cardinality * r_inputPages;
    elif (self.joinMethod == "block-nested-loops"):
      pageBlockNum = math.ceil(l_tmpFileSize/ self.storage.bufferPool.poolSize);
      local_cost = l_inputPages + pageBlockNum * r_inputPages;
    # We don't support indexed
    # elif (self.joinMethod == "indexed"):
        # index_pages = self.storage.fileMgr.getIndex(self.indexID).numPages(); Not verified with BDB index file
        # rmatch_pages = ?
        # local_cost = l_inputPages + self.lhsPlan.cardinality * (index_pages + rmatch_pages);
    
    elif (self.joinMethod == "hash"):
      local_cost = 3 * (l_inputPages + r_inputPages);
    return local_cost;
        
class Groupby:  
  def localCost(self, estimated):
    inputPages = self.subPlan.tempFile.numPages();
    if estimated:
      inputPages *= self.sampleFactor;
    local_cost = 3 * inputPages;
    return local_cost;