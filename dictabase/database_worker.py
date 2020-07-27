import time
from collections import defaultdict
import threading
import sys
import dataset
from dictabase.helpers import LoadKeys, DumpKeys, IsSubset


class DatabaseWorker:
    # this is the only object/thread that should interact with the database

    def __init__(self):
        self._dburi = None
        self._db = None

        self._inUse = defaultdict(dict)  # use dict of dicts for fast lookups
        self._alreadyDeletedQ = defaultdict(dict)

        self._workerLock = threading.Lock()

        self._debug = False

    def SetDebug(self, newState):
        self._debug = newState

    def print(self, *a, **k):
        if self._debug:
            print('DatabaseWorker._inUse=', self._inUse)
            print(*a, **k)

    def RegisterDBURI(self, dburi):
        self.print('RegisterDBURI(', dburi)
        if dburi is None:
            if sys.platform.startswith('win'):
                dburi = 'sqlite:///MyDatabase.db'
            else:  # linux
                dburi = 'sqlite:////MyDatabase.db'

        self._dburi = dburi
        self._db = dataset.connect(
            self._dburi,
            engine_kwargs={'connect_args': {'check_same_thread': False}} if 'sqlite' in self._dburi else None
            # to avoid error; ProgrammingError: SQLite objects created in a thread can only be used in that ame thread.The object was created in thread id 23508 and this is thread id 640
        )

    def Insert(self, cls, **kwargs):
        self.print('Insert', cls, kwargs)

        obj = cls(**kwargs)

        with self._workerLock:
            self._db.begin()
            tableName = type(obj).__name__  # do this before DumpKeys
            dumpedObj = DumpKeys(obj)
            d = dict(dumpedObj)
            ID = self._db[tableName].insert(dumpedObj)
            self._db.commit()

            obj['id'] = ID
            self.AddToInUse(obj)

        return obj

    def Drop(self, cls):
        self.print('Drop(', cls)

        self._CommitAll()

        with self._workerLock:
            self._db.begin()
            tableName = cls.__name__
            self._db[tableName].drop()
            self._db.commit()

    def AddToInUse(self, obj):
        self.print('AddToInUseQ(', obj)
        self._inUse[type(obj)][obj['id']] = obj

    def Upsert(self, obj, keepInUse=False):
        self.print('Upsert(', obj, ',keepInUse=', keepInUse)
        # this is called when there are no more references to a BaseTable() object (aka obj.__del__() is called)

        if not keepInUse:
            # pop obj from inUse
            self._inUse[type(obj)].pop(obj['id'], None)

        alreadyDeletedObj = self._alreadyDeletedQ[type(obj)].pop(obj['id'], None)
        if alreadyDeletedObj:
            return  # dont commit this obj. its been deleted

        with self._workerLock:
            tableName = type(obj).__name__  # do this before DumpKeys
            obj = DumpKeys(obj)
            self._db.begin()
            d = dict(obj)
            self._db[tableName].upsert(d, ['id'])  # find row with matching 'id' and update it
            self._db.commit()

    def Delete(self, obj):
        self.print('Delete(', obj)
        self._CommitAll()

        with self._workerLock:
            self._db.begin()
            tableName = type(obj).__name__
            d = {'id': obj['id']}
            self._db[tableName].delete(**d)
            self._db.commit()

        self._alreadyDeletedQ[type(obj)][obj['id']] = obj

        self.print('Delete complete for obj=', obj)

    def FindOne(self, cls, kwargs):
        self.print('FindOne(', cls, kwargs)

        # if this object is already in use, return the reference

        with self._workerLock:
            for obj in self._inUse[cls].values():
                if IsSubset(superDict=obj, subDict=dict(**kwargs)):
                    self.print('FindOne return from inUse obj=', obj)
                    return obj

        self._CommitAll(keepInUse=True)

        # self._db.begin() # dont do this

        with self._workerLock:

            tableName = cls.__name__
            tbl = self._db[tableName]
            ret = tbl.find_one(**kwargs)
            if ret:
                ret = cls(**ret)
                ret = LoadKeys(ret)
                self.AddToInUse(ret)
            else:
                ret = None

        return ret

    def FindAll(self, cls, kwargs):
        self.print('FindAll(', cls, kwargs)

        with self._workerLock:
            foundInUse = []
            for obj in self._inUse[cls].values():
                if IsSubset(superDict=obj, subDict=dict(**kwargs)):
                    foundInUse.append(obj)

        self._CommitAll(keepInUse=True)

        # special kwargs
        reverse = kwargs.pop('_reverse', False)  # bool
        orderBy = kwargs.pop('_orderBy', None)  # str
        if reverse is True:
            if orderBy is not None:
                orderBy = '-' + orderBy
            else:
                orderBy = '-id'

        # do look up
        with self._workerLock:
            tableName = cls.__name__

            if len(kwargs) == 0:
                if tableName not in self._db:
                    newTable = self._db[tableName]  # create a new table

                foundInDB = self._db[tableName].all(order_by=[f'{orderBy}'])
            else:
                if orderBy is not None:
                    foundInDB = self._db[tableName].find(
                        order_by=['{}'.format(orderBy)],
                        **kwargs
                    )
                else:
                    foundInDB = self._db[tableName].find(**kwargs)

        # yield type-cast items one by one
        alreadyYielded = set()  # set() of int(id)
        self.print('foundInUse=', foundInUse)

        for obj in foundInUse:  # in use has priority
            self.AddToInUse(obj)
            alreadyYielded.add(obj['id'])
            self.print('FindAll foundInUse yield obj=', obj)
            yield obj

        for d in foundInDB:
            self.print('foundInDB d=', d)
            self.print('alreadyYielded=', alreadyYielded)

            if d['id'] not in alreadyYielded:
                obj = cls(**d)
                obj = LoadKeys(obj)
                self.AddToInUse(obj)
                alreadyYielded.add(obj['id'])
                self.print('FindAll foundInDB yield obj=', obj)
                yield obj

    def _CommitAll(self, keepInUse=False):
        self.print('CommitAll(keepInUse=', keepInUse)

        for theType in self._inUse:
            for obj in self._inUse[theType].copy().values():
                self.Upsert(obj, keepInUse=keepInUse)
