class Request(object):
    def __init__(self,transaction,x,v,operation,success,callback):
        self.transaction = transaction
        self.x = x
        self.v = v
        self.operation = operation
        self.success = success
        self.callback = callback