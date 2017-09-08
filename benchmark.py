import time
import logging

logger = logging.getLogger(__name__)

class benchmark(object):

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = time.time()

    def __exit__(self,ty,val,tb):
        end = time.time()
        logger.debug("%s : %0.2f seconds" % (self.name, end-self.start))
        return False