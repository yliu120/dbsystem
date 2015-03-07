import heapq;

class kWayMergeSort:
    
    def __init__(self, listOfGenerators):
        
        self.listOfGenerators = listOfGenerators;
        if self.listOfGenerators:
            self.heap  = [];
            for g in self.listOfGenerators:
                heapq.heappush( self.heap, (next(g), g) );
            self.value = [];
            
    def sort(self):
        
        while ( self.heap != [] ):
            
            (value, g) = heapq.heappop(self.heap);
            try:
                nextValue = next(g);
                heapq.heappush(self.heap, (nextValue, g));
            except StopIteration:
                pass

            self.value.append(value);
            
        yield self.value;
            
   
list1 = [(1,1),(1,2),(3,3),(4,5),(6,7)]
list2 = [(2,3),(3,5),(4,7),(6,9),(7,7)]
list3 = [(4,6),(9,8)]

mSort = kWayMergeSort([iter(list1), iter(list2), iter(list3)]);
print([sortOut for sortOut in mSort.sort()]);         
        