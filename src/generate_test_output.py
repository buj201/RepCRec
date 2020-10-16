from io import StringIO
import sys
import glob

from .TransactionManager import TransactionManager

tests = glob.glob('tests/provided_tests/t*')

for t in tests:
    try:
        TM.instructions.close()
    except:
        pass
    TM = TransactionManager(test=t)
    
    buff = StringIO()
    # Replace default stdout (terminal) with our stream
    sys.stdout = buff
    
    TM.main()
    
    with open(f"tests/provided_test_outputs/{t.split('/')[-1]}",'w') as f:
        f.write(buff.getvalue())
        