
import ezlog

import multiprocessing as multiproc

import itertools



################################################################################
#                               setup logging                                  #
################################################################################

_logger = ezlog.setup(__name__)


################################################################################
#                               Process pool                                   #
################################################################################


class Pool(object):
    def __init__(self, processes=None, initializer=None, initargs=(), maxtasksperchild=None):
        self.nprocs = processes or 1

        if not self.nprocs == 1:
            self.pool = multiproc.Pool(processes=processes, initializer=initializer, initargs=initargs, maxtasksperchild=maxtasksperchild)
        else:
            self.pool = None

    def map(self, func, iterable, chunksize=1, force=False):
        """
        Map a function over the iterable.

        @param func (function): the function to apply
        @param iterable (anything implementing __iter__()): the sequence of values
        @param chunksize=1 (int): the chunksize when using multiple processors
        @param force=False (boolean): force evaluation of the result when using single processor, otherwise (default) use lazy paradigm

        @return (list or generator): a list (nprocs > 1 or force == True) or a generator (nprocs == 1) of values from applying the function to the iterable
        """
        _logger.debug('mapping using %s processors' % self.nprocs)

        if self.nprocs == 1:
            generator = itertools.imap(func, iterable)
            if force:
                return list(generator)
            else:
                return generator
        else:
            return self.pool.map(func, iterable, chunksize)

    def finish(self):
        """
        Clear up the pool if using multiple processors
        """
        if not self.pool is None:
            self.pool.close()
            self.pool.join()
            self.pool.terminate()

    def __del__(self):
        self.finish()
        self.pool = None



_DEFAULT_POOL = Pool(processes=1)

def setup_pool(processes):
    """
    Setup the default module-level process pool.

    @param processes (int): the number of processors to use.

    Example:
     import fax
     nprocs = 42
     fax.setup_pool(nprocs)
     ...
     project = fax.load_project(root)
    """

    global _DEFAULT_POOL
    _DEFAULT_POOL = Pool(processes=processes)


def get_pool(pool):
    """
    Chooses between the module default Pool or the provided one if it is of type Pool

    @param pool (Pool or anything): if type(pool) is Pool return pool else return DEFAULT_POOL
    @return (Pool)
    """

    if type(pool) is Pool:
        mypool = pool
        _logger.debug('Using provided Pool(%s)' % mypool.nprocs)
        return mypool
    else:
        global _DEFAULT_POOL
        mypool = _DEFAULT_POOL
        _logger.debug('Using default Pool(%s)' % mypool.nprocs)
        return mypool


def map(*args, **kws):
    """
    Wrapper for Pool.map using the default Pool instance
    @params *args, **kws: passed directly to Pool.map
    """

    return get_pool(None).map(*args, **kws)
