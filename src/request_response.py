class RequestResponse(object):
    """Central object for message passing. As a request, this
    class encapsulates:
        - The requesting transaction
        - The function that needs to be called to attempt to
          execute the response.
        - The variable targeted by the request (if any)
    As a response, this class encapsulates:
        - The return value
        - The return status.
    """
    def __init__(self,transaction,x,v,operation,success,callback):
        """
        Parameters
        ----------
        transaction : Transaction
            Transaction making request.
        x : str (variable name) or None
            Variable targeted by read/write request (None for other requests)
        v : Any
            Response to read requests.
        operation : str
            Operation name (W,R,begin,etc.)
        success : bool
            Status of response
        callback : function
            Function to call to attempt request.
        """
        self.transaction = transaction
        self.x = x
        self.v = v
        self.operation = operation
        self.success = success
        self.callback = callback