from fuzzer_base import *
from gp.client import *
from test_config import *
import sys

fuzz_commands = ( "list-roots", "list-successors", "add-arcs", "stats",
  "22", "-11111", "xyz", "0", "!", "&", "%", "#", "",)

fuzz_args = ( "1", "2", "11", "22", "-11111", "xyz", "0", "!",
  "&", "%", "#", "",)


class CrudFuzzer (FuzzerBase):
    """Test the TCP client connection"""
    
    def prepare(self):
        #self.gp.add_arcs(((1, 2), (1, 11), (2, 22),))
        self.gp.add_arcs(((1, 2), (1, 11), (2, 22),))
        


    def doFuzz(self):
        global fuzz_commands
        global fuzz_args

        cmd = ""

        cmd = cmd + fuzz_pick(fuzz_commands)
        cmd = cmd + " "
        cmd = cmd + fuzz_pick(fuzz_args)
        cmd = cmd + " "
        cmd = cmd + fuzz_pick(fuzz_args)
            
        try:
            self.gp.execute(cmd)
            return True
        except gpProcessorException, ex:
            pass # noop
        except gpUsageException, ex:
            pass # noop
            
        return False

if __name__ == '__main__':

	fuzzer = CrudFuzzer()

	fuzzer.run(sys.argv)
