import gc
import time
import copy
from collections import defaultdict

import dataset
import sys
import sys

DEBUG = True
if DEBUG is False or sys.platform.startswith('linux'):
    print = lambda *a, **k: None


def RegisterDBURI(dburi=None):
    global _DB
    global _DBURI
    if dburi is None:
        if sys.platform.startswith('win'):
            dburi = 'sqlite:///MyDatabase.db'
        else:  # linux
            dburi = 'sqlite:////MyDatabase.db'

    _DBURI = dburi
    _DB = dataset.connect(dburi)
    print('_DB=', _DB)


class BaseTable(dict):
    def LoadKey(self, key, dbValue):
        # moving data from database to the BaseTable object
        return dbValue

        # use below template
        # return {
        #     'pages': lambda v: json.loads(v),
        # }.get(key, lambda v: v)(dbValue)

    def DumpKey(self, key, value):
        # moving data from the BaseTable object to the database
        return value

        # use below as template
        # return {
        #     'pages': lambda v: json.dumps(v, indent=2, sort_keys=True),
        # }.get(key, lambda v: v)(objValue)

    def __del__(self):
        print(f'{self}.__del__()')
        _DBManager.MoveToCommitQ(self)

    def __str__(self):
        '''

        :return: string like '<BaseDictabaseTable: email=me@website.com, name=John>'
        '''
        itemsList = []
        for k, v, in self.items():
            if k.startswith('_'):
                if DEBUG is False:
                    continue  # dont print these

            if isinstance(v, str) and len(v) > 25:
                v = v[:25] + '...'
            itemsList.append(('{}={}(type={})'.format(k, v, type(v).__name__)))

        if DEBUG:
            itemsList.append(('{}={}'.format('pyid', id(self))))

        return '<{}: {}>'.format(
            type(self).__name__,
            ', '.join(itemsList)
        )

    def __repr__(self):
        return str(self)


def New(cls, **kwargs):
    inserting = True
    print('New(', cls, kwargs)
    CommitAll()
    print('New(', cls, kwargs)
    for k, v in kwargs.items():
        if isinstance(v, bytes):
            raise TypeError('Don\'t use type "bytes". Use b"data".encode()')
        elif isinstance(v, list) or isinstance(v, dict):
            if not cls.LoadKey or not cls.DumpKey:
                raise TypeError(
                    'Type {} cannot be stored natively. Please override {}.LoadKey and {}.DumpKey to convert to a database-safe type. Think json.dumps() and json.loads()'.format(
                        type(v),
                        type(v),
                        type(v),
                    ))

    newObj = cls(**kwargs)
    newID = _DBManager.Insert(newObj)
    newObj['id'] = newID
    _DBManager.AddToInUseQ(newObj)
    return newObj


def Drop(cls, confirm=False):
    _DBManager.CommitAll()
    if confirm:
        _DBManager.Drop(cls)
    else:
        raise PermissionError('Cannot drop table "{}" unless you pass the kwarg confirm=True.'.format(cls.__name__))


def Delete(obj):
    print('Delete(', obj)
    _DBManager.CommitAll()
    _DBManager.MoveToDeleteQ(obj)
    _DBManager.WaitForProcessingToStop()


def FindOne(cls, **kwargs):
    # _DB.begin() # dont do this
    CommitAll()

    ret = _DBManager.FindOne(cls, **kwargs)
    if ret:
        return ret

    tableName = cls.__name__
    tbl = _DB[tableName]
    ret = tbl.find_one(**kwargs)
    print('tbl.find_one(', kwargs, '; ret=', ret)
    if ret:
        ret = cls(**ret)
        ret = _LoadKeys(ret)
        _DBManager.AddToInUseQ(ret)
        return ret
    else:
        return None


def FindAll(cls, **kwargs):
    CommitAll()

    ret = _DBManager.FindAll(cls, **kwargs)

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
        ret += _DB[tableName].all(order_by=[f'{orderBy}'])
    else:
        if orderBy is not None:
            ret += _DB[tableName].find(
                order_by=['{}'.format(orderBy)],
                **kwargs
            )
        else:
            ret += _DB[tableName].find(**kwargs)

    # yield type-cast items one by one
    # _DBManager.SetProcess(True)
    print('FindAll(', cls, kwargs, '; ret=', list(ret))
    for d in ret:
        obj = cls(**d)
        obj = _LoadKeys(obj)
        _DBManager.AddToInUseQ(obj)
        yield obj


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


def CommitAll():
    _DBManager.CommitAll()


class _DatabaseManager:
    def __init__(self):
        self._inUseQ = defaultdict(dict)  # {
        # type(): {
        # int(ID): BaseTable(obj) # or child
        # }
        self._commitQ = defaultdict(dict)
        self._deleteQ = defaultdict(dict)
        self._alreadyDeletedQ = defaultdict(dict)
        self._processing = False
        self._shouldProcess = True
        self.inserting = False

    def _PrintQs(self):
        print('self._inUseQ=')
        for theType in self._inUseQ:
            for obj in self._inUseQ[theType].values():
                print('    ', obj)

        print('self._commitQ=')
        for theType in self._commitQ:
            for obj in self._commitQ[theType].copy().values():
                print('    ', obj)

        print('self._deleteQ=')
        for theType in self._deleteQ:
            for obj in self._deleteQ[theType].values():
                print('    ', obj)

    def AddToInUseQ(self, obj):
        print('AddToInUseQ{', obj)
        self._inUseQ[type(obj)][obj['id']] = obj
        self._PrintQs()

    def CommitAll(self):
        print('CommitAll()')
        self._PrintQs()

        for theType in self._inUseQ:
            for obj in self._inUseQ[theType].copy().values():
                self.MoveToCommitQ(obj)

        self.WaitForProcessingToStop()
        self._PrintQs()
        print('CommitAll() end')

    def MoveToCommitQ(self, obj):
        print('MoveToCommitQ(', obj)
        self._PrintQs()

        self._inUseQ[type(obj)].pop(obj['id'], None)

        alreadyDeletedObj = self._alreadyDeletedQ[type(obj)].pop(obj['id'], None)
        if alreadyDeletedObj:
            return  # dont commit this obj. its been deleted

        self._commitQ[type(obj)][obj['id']] = obj

        self._PrintQs()
        if self._shouldProcess:
            self.SetShouldProcess(True)

    def FindOne(self, cls, **kwargs):
        for obj in self._inUseQ[cls].values():
            if IsSubset(subDict=kwargs, superDict=obj):
                print('DBM.FindOne(', cls, kwargs, '; ret=', obj)
                return obj

        for obj in self._commitQ[cls].values():
            if IsSubset(subDict=kwargs, superDict=obj):
                print('DBM.FindOne(', cls, kwargs, '; ret=', obj)
                return obj

        print('DBM.FindOne(', cls, kwargs, '; ret=None')

    def FindAll(self, cls, **kwargs):
        ret = set()

        for obj in self._inUseQ[cls].values():
            if IsSubset(subDict=kwargs, superDict=obj):
                ret.append(obj)

        for obj in self._commitQ[cls].values():
            if IsSubset(subDict=kwargs, superDict=obj):
                ret.append(obj)

        print('DBM.FindAll(', cls, kwargs, '; ret=', ret)
        return list(ret)

    def Insert(self, obj):
        print('DBM.Insert(', obj)

        tableName = type(obj).__name__  # do this before DumpKeys
        obj = _DumpKeys(obj)
        print('Insert obj after DumpKeys=', obj)

        self.SetShouldProcess(False)

        self.WaitForProcessingToStop()

        _DB.begin()
        d = dict(obj)
        print('_Insert d=', d, ', tableName=', tableName)
        ID = _DB[tableName].insert(d)
        _DB.commit()

        self.SetShouldProcess(True)
        print('_Insert d=', d, '; return ID=', ID)
        return ID

    def WaitForInsertToComplete(self):
        print('WaitForInsertToComplete()')
        while self.inserting:
            pass
        print('Inserting Complete')

    def MoveToDeleteQ(self, obj):
        print('MoveToDeleteQ(', obj)
        self._deleteQ[type(obj)][obj['id']] = obj

        self._PrintQs()
        if self._shouldProcess:
            self.SetShouldProcess(True)

    def Delete(self, obj):
        print('DBM.Delete(', obj)

        _DB.begin()
        dbName = type(obj).__name__
        _DB[dbName].delete(**obj)
        _DB.commit()

        self._alreadyDeletedQ[type(obj)][obj['id']] = obj

    def Drop(self, cls):
        self.SetShouldProcess(False)
        self.WaitForProcessingToStop()

        _DB.begin()
        tableName = cls.__name__
        _DB[tableName].drop()
        _DB.commit()

        self.SetShouldProcess(True)

    def SetShouldProcess(self, state):
        self._shouldProcess = state
        if self._shouldProcess and not self._processing:
            self._ProcessOneFromQueue()

    def WaitForProcessingToStop(self):
        print('DBM.WaitForProcessingToStop()')
        while self._processing:
            pass  # wait for the processing to stop
        print('Processing Stopped')

    def _ProcessOneFromQueue(self):
        print('DBM._ProcessOneFromQueue()')
        self._processing = True

        for theType in self._deleteQ:
            if not self._shouldProcess:
                break
            for obj in self._deleteQ[theType].copy().values():
                if not self._shouldProcess:
                    break

                self._inUseQ[theType].pop(obj['id'], None)  # remove from inUseQ if exists
                self._commitQ[theType].pop(obj['id'], None)  # remove from inUseQ if exists

                print('_ProcessOneFromQueue.Delete(', obj)
                self.Delete(obj)
                self._deleteQ[theType].pop(obj['id'], None)

        for theType in self._commitQ:
            if not self._shouldProcess:
                break
            for obj in self._commitQ[theType].copy().values():
                if not self._shouldProcess:
                    break

                print('_ProcessOneFromQueue.Upsert(', obj)

                self._Upsert(obj)
                self._commitQ[theType].pop(obj['id'], None)

        self._processing = False

    def _Upsert(self, obj):
        print('_Upsert(', obj)
        tableName = type(obj).__name__  # do this before DumpKeys

        obj = _DumpKeys(obj)
        print('_Upsert obj after DumpKey=', obj)

        _DB.begin()

        d = dict(obj)
        print('_Insert d=', d, ', tableName=', tableName)
        _DB[tableName].upsert(d, ['id'])  # find row with matching 'id' and update it

        _DB.commit()
        print('_Upsert end')


_DBManager = _DatabaseManager()


def IsSubset(subDict, superDict):
    return all(item in superDict.items() for item in subDict.items())
