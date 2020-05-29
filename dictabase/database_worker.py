import time
from collections import defaultdict
import threading
import sys
import dataset
import logging

logger = logging.getLogger('db_transactions')


class DatabaseWorker:
    # this is the only object/thread that should interact with the database

    def __init__(self):
        self._dburi = None
        self._db = None

        self._insertQ = []  # list of tuples [(cls, **kwargs, callback), ]
        self._dropQ = []  # list of tuples [(cls, callback), ]
        self._findOneQ = []  # list of tuples [(cls, kwargs, callback), ]
        self._findAllQ = []  # list of tuples [(cls, kwargs, callback), ]
        self._inUseQ = defaultdict(dict)  # use dict of dicts for fast lookups
        self._commitQ = defaultdict(dict)
        self._deleteQ = defaultdict(dict)
        self._alreadyDeletedQ = defaultdict(dict)

        self._workerThread = None
        self._workerLock = threading.Lock()
        self._toDoLock = threading.Lock()
        self._commitLock = threading.Lock()

        self._debug = False
        self._doWork = True

    def SetDebug(self, newState):
        self._debug = newState

    def print(self, *a, **k):
        if self._debug:
            print(*a, **k)

    def RegisterDBURI(self, dburi):
        self.print('RegisterDBURI(', dburi)
        if dburi is None:
            if sys.platform.startswith('win'):
                dburi = 'sqlite:///MyDatabase.db'
            else:  # linux
                dburi = 'sqlite:////MyDatabase.db'

        self._dburi = dburi
        self._db = dataset.connect(self._dburi)

        self._RestartWorker()

    def _RestartWorker(self):
        if self._workerThread is None or not self._workerThread.is_alive():
            self._workerThread = threading.Timer(0, self._DoWork)
            self._workerThread.start()
        self.print('_RestartWorker workerThread=', self._workerThread, 'is_alive=', self._workerThread.is_alive())

    def New(self, cls, _callback=None, **kwargs):
        self._insertQ.append((cls, kwargs, _callback))
        self._RestartWorker()

    def _Insert(self, obj):
        self._db.begin()
        d = dict(obj)
        tableName = type(obj).__name__
        logger.debug(f'insert {d}')
        ID = self._db[tableName].insert(d)
        self._db.commit()
        obj['id'] = ID
        return obj

    def AddToDropQ(self, cls, callback):
        self.print('AddToDrop(', cls, callback)
        self._CommitAll()
        self._dropQ.append((cls, callback))
        self._RestartWorker()

    def _Drop(self, cls):
        self._db.begin()
        tableName = cls.__name__
        logger.debug(f'drop {cls}')
        self._db[tableName].drop()
        self._db.commit()

    def AddToInUseQ(self, obj):
        self.print('AddToInUseQ(', obj)
        self._inUseQ[type(obj)][obj['id']] = obj
        self._RestartWorker()

    def AddToCommitQ(self, obj):
        self.print('AddToCommitQ(', obj)
        # this is called when there are no more references to a BaseTable() object (aka obj.__del__() is called)
        self._inUseQ[type(obj)].pop(obj['id'], None)

        alreadyDeletedObj = self._alreadyDeletedQ[type(obj)].pop(obj['id'], None)
        if alreadyDeletedObj:
            return  # dont commit this obj. its been deleted

        self._commitQ[type(obj)][obj['id']] = obj
        self._RestartWorker()

    def _Upsert(self, obj):
        tableName = type(obj).__name__  # do this before DumpKeys
        obj = _DumpKeys(obj)
        self._db.begin()
        d = dict(obj)
        logger.debug(f'upsert {d}')
        self._db[tableName].upsert(d, ['id'])  # find row with matching 'id' and update it
        self._db.commit()

    def AddToDeleteQ(self, obj, _callback):
        self.print('AddToDeleteQ(', obj, _callback)
        self._CommitAll()
        self._deleteQ[type(obj)][obj['id']] = (obj, _callback)
        self._RestartWorker()

    def _Delete(self, obj):
        self._db.begin()
        tableName = type(obj).__name__
        logger.debug(f'delete {obj}')
        self._db[tableName].delete(**obj)
        self._db.commit()

        self._alreadyDeletedQ[type(obj)][obj['id']] = obj

    def FindOne(self, cls, kwargs, callback):
        self.print('FindOne(', cls, kwargs, callback)
        self._CommitAll()
        self._findOneQ.append((cls, kwargs, callback))
        self._RestartWorker()

    def _DoFindOne(self, cls, kwargs):
        # self._db.begin() # dont do this
        tableName = cls.__name__
        tbl = self._db[tableName]
        logger.debug(f'find_one {kwargs}')
        ret = tbl.find_one(**kwargs)
        if ret:
            ret = cls(**ret)
            ret = _LoadKeys(ret)
            self.AddToInUseQ(ret)
            return ret
        else:
            return None

    def FindAll(self, cls, kwargs, callback):
        self.print('FindAll(', cls, kwargs, callback)
        self._CommitAll()
        self._findAllQ.append((cls, kwargs, callback))
        self._RestartWorker()

    def _DoFindAll(self, cls, kwargs):
        # special kwargs
        reverse = kwargs.pop('_reverse', False)  # bool
        orderBy = kwargs.pop('_orderBy', None)  # str
        if reverse is True:
            if orderBy is not None:
                orderBy = '-' + orderBy
            else:
                orderBy = '-id'

        # do look up
        tableName = cls.__name__

        if len(kwargs) == 0:
            ret = self._db[tableName].all(order_by=[f'{orderBy}'])
        else:
            if orderBy is not None:
                ret = self._db[tableName].find(
                    order_by=['{}'.format(orderBy)],
                    **kwargs
                )
            else:
                ret = self._db[tableName].find(**kwargs)

        # yield type-cast items one by one
        for d in ret:
            obj = cls(**d)
            obj = _LoadKeys(obj)
            self.AddToInUseQ(obj)
            yield obj

    def _CommitAll(self):
        self.print('CommitAll')
        self._commitLock.acquire()

        for theType in self._inUseQ:
            for obj in self._inUseQ[theType].copy().values():
                self.AddToCommitQ(obj)

        self._RestartWorker()

        while not IsEmpty(self._commitQ):
            pass

        self.print('CommitAll commitQ is empty')

        while not IsEmpty(self._deleteQ):
            pass

        self.print('CommitAll _deleteQ is empty')

        while self._dropQ:
            pass

        self.print('CommitAll _dropQ is empty')

        while self._insertQ:
            pass

        self.print('CommitAll _insertQ is empty')
        self.print('CommitAll end')
        self._commitLock.release()

    def _WorkToDo(self):
        self._toDoLock.acquire()
        for thisList in [
            self._insertQ,
            self._dropQ,
            self._findOneQ,
            self._findAllQ,
        ]:
            if thisList:
                self.print('WorkToDo() ret True')
                self._toDoLock.release()
                return True

        for thisDict in [
            # self._inUseQ, # there is no work to be done with inUseQ, its just holding items until they are placed in the commit Q
            self._commitQ,
            self._deleteQ,
        ]:
            if not IsEmpty(thisDict):
                self.print('WorkToDo() ret True')
                self._toDoLock.release()
                return True

        # self.PrintQs()
        self.print('WorkToDo() ret False')
        # self.PrintQs()
        self._toDoLock.release()
        return False

    def PrintQs(self):
        self.print('self._findOneQ=', self._findOneQ)
        self.print('self._findAllQ=', self._findAllQ)
        self.print('self._insertQ=', self._insertQ)
        self.print('self._deleteQ=', self._deleteQ)
        self.print('self._dropQ=', self._dropQ)
        self.print('self._commitQ=', self._commitQ)
        self.print('self.inUseQ=', self._inUseQ)

    def _DoWork(self):
        self.print('_DoWork()')
        # this method will run forever (while True)

        if self._db:
            self.print('self._db=', self._db)

            while self._doWork:#self._WorkToDo():
                self._workerLock.acquire()
                self.print('\n\n\n_DoWork*****************************************************')
                self.PrintQs()
                # do any FindOne operations
                while self._findOneQ:
                    cls, kwargs, callback = self._findOneQ.pop(0)
                    result = self._DoFindOne(cls, kwargs)
                    callback(result)

                # do any FindAll operations
                while self._findAllQ:
                    cls, kwargs, callback = self._findAllQ.pop(0)
                    result = self._DoFindAll(cls, kwargs)
                    callback(result)

                # insert any new objects
                while self._insertQ:
                    cls, kwargs, callback = self._insertQ.pop(0)
                    newObj = cls(**kwargs)
                    newObj = self._Insert(newObj)
                    callback(newObj)
                    self.AddToInUseQ(newObj)

                # delete any objects
                for theType in self._deleteQ:
                    for obj, callback in self._deleteQ[theType].copy().values():
                        # if this obj should be deleted, then we dont want it to be in the inUseQ or commitQ
                        self._inUseQ[theType].pop(obj['id'], None)  # remove from inUseQ if exists
                        self._commitQ[theType].pop(obj['id'], None)  # remove from _commitQ if exists

                        self._Delete(obj)
                        self._deleteQ[theType].pop(obj['id'], None)
                        callback()

                # drop any tables
                while self._dropQ:
                    cls, callback = self._dropQ.pop(0)
                    self._Drop(cls)
                    callback()

                # commit any objects
                for theType in self._commitQ:
                    for obj in self._commitQ[theType].copy().values():
                        self._Upsert(obj)
                        self._commitQ[theType].pop(obj['id'], None)

                self._workerLock.release()

                while not self._WorkToDo():
                    time.sleep(1)

    def __del__(self):
        self._doWork = False


def _LoadKeys(obj):
    for k, v in obj.copy().items():
        obj[k] = obj.LoadKey(k, v)
    return obj


def _DumpKeys(obj):
    baseTableObj = obj
    dictObj = baseTableObj.copy()
    for k, v in baseTableObj.items():
        dictObj[k] = baseTableObj.DumpKey(k, v)
    return dictObj


def IsEmpty(d):
    isEmpty = True
    for theType in d:
        if len(d[theType]) > 0:
            isEmpty = False
            break

    return isEmpty
