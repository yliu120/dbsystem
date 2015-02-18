##################################################
# Class : Adder 
# similar to workloadgenerator

class Adder:

  def __init__(self, x, y):
    self.x = x;
    self.y = y;

  def sum(self):
    
    result = 0;
    for i in range(self.x, self.y + 1):
       result += i;
    
    return result; 
