from collections import namedtuple

Response = namedtuple('Response',['transaction','x','v','operation','success','callback'])

WriteOp = namedtuple('WriteOp',['x','v'])
ReadOp = namedtuple('ReadOp',['x'])
