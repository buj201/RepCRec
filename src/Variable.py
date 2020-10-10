class Variable(object):
    
    def __init__(self,var_num,replicated):
        self.name = f'x{var_num}'
        self.value = 10*var_num
        self.replicated = replicated
        self.available = True
        
        self.before_available = True
        self.before_image = self.value
    
    def read(self):
        """
        Returns self.value. Caller's responsibility to check locks.
        
        Args:
            None
            
        Return:
            self.value
            
        Side effects:
            None
        """
        return self.value
    
    def write(self,v):
        """
        Updates self.value. Caller's responsibility to check locks.
        
        Args:
            v: new value
        
        Returns:
            None
            
        Side effects:
            - Stores self.value as self.before_image
            - Updates self.value = v
            - Updates self.available to True
        """
        self.before_available = self.available
        self.before_image = self.value
        self.value = v
        self.available = True
        
    def undo_write(self):
        """
        Undo a write when a transaction is aborted.
        
        
        Args:
            None
            
        Returns:
            None
            
        Side effects:
            - Reset self.value = self.before_image
            - Reset availalble = self.before_available
        """
        self.value = self.before_image
        self.available = self.before_available