from collections import namedtuple

ReadResponse = namedtuple('ReadResponse',['success','value','callback','transaction'])
WriteResponse = namedtuple('WriteResponse',['success','callback','transaction'])

WriteOp = namedtuple('WriteOp',['x','v'])
ReadOp = namedtuple('ReadOp',['x'])
