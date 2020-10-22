from io import StringIO
import sys
import glob
import pandas as pd

from .TransactionManager import TransactionManager

tests = glob.glob('tests/provided_tests/t*')

for t in tests:
    try:
        TM.instructions.close()
    except:
        pass
    TM = TransactionManager(test=t)
    
    sys.stdout = sys.__stdout__
    print(f'Running test {t}:')
    
    buff = StringIO()
    # Replace default stdout (terminal) with our stream
    sys.stdout = buff
    
    TM.main()
    
    output_lines = [x.strip() for x in buff.getvalue().split('\n') if len(x) > 0]

    with open(f"tests/provided_test_outputs/{t.split('/')[-1]}",'r') as f:
        target_output_lines = [x.strip() for x in f.readlines() if len(x) > 0]
    
    if output_lines != target_output_lines:
        
        sys.stdout = sys.__stdout__
        print(f'Test {t} failed. Diff:')
        r = pd.concat([pd.Series(target_output_lines),
                         pd.Series(output_lines)],axis=1)
        r.columns = ['Target output','Actual output']
        print(r)
        #raise ValueError('Test failed. Aborting.')
    else:
        sys.stdout = sys.__stdout__
        print(f'\tTest {t} passed!')