import dictabase.base_table
from dictabase.database_worker import DatabaseWorker
import time

DEBUG = True

_dbWorker = DatabaseWorker()
_dbWorker.SetDebug(DEBUG)

dictabase.base_table.RegisterDbWorker(_dbWorker)
dictabase.base_table.SetDebug(DEBUG)


def RegisterDBURI(dburi=None):
    _dbWorker.RegisterDBURI(dburi)


def New(cls, **kwargs):
    newObj = None

    def Callback(newObjWithID):
        nonlocal newObj
        newObj = newObjWithID

    _dbWorker.New(cls, _callback=Callback, **kwargs)

    while newObj is None:
        time.sleep(0.1)

    return newObj


def Delete(obj):
    deleteComplete = False

    def CallbackDeleteComplete():
        nonlocal deleteComplete
        deleteComplete = True

    _dbWorker.AddToDeleteQ(obj, _callback=CallbackDeleteComplete)

    while not deleteComplete:
        time.sleep(0.1)


def Drop(cls, confirm=False):
    if confirm:
        dropComplete = False

        def CallbackDropComplete():
            nonlocal dropComplete
            dropComplete = True

        _dbWorker.AddToDropQ(cls, CallbackDropComplete)

        while not dropComplete:
            time.sleep(0.1)
    else:
        raise PermissionError('Cannot drop table "{}" unless you pass the kwarg "confirm=True".'.format(cls.__name__))


def FindOne(cls, **kwargs):
    findOneResult = False

    def CallbackFindOneComplete(result):
        nonlocal findOneResult
        findOneResult = result

    _dbWorker.FindOne(cls, kwargs, CallbackFindOneComplete)

    while findOneResult is False:
        time.sleep(0.1)

    return findOneResult


def FindAll(cls, **kwargs):
    findAllResult = False

    def CallbackFindAllComplete(result):
        nonlocal findAllResult
        findAllResult = result

    _dbWorker.FindAll(cls, kwargs, CallbackFindAllComplete)

    while findAllResult is False:
        time.sleep(0.1)

    return findAllResult
