import dictabase.base_table
from dictabase.database_worker import DatabaseWorker
import time
from dictabase.base_table import BaseTable
from dictabase.helpers import ExponentialDelay
import subprocess

DEBUG = True
oldPrint = print

_dbWorker = DatabaseWorker()
dictabase.base_table.RegisterDbWorker(_dbWorker)


def SetDebug(newState, thisModule=True):
    global print
    dictabase.base_table.SetDebug(newState)
    _dbWorker.SetDebug(newState)
    if thisModule:
        if newState is False:
            print = lambda *a, **k: None
        else:
            print = oldPrint


def RegisterDBURI(dburi=None):
    _dbWorker.RegisterDBURI(dburi)


def New(cls, **kwargs):
    print('New(', cls, kwargs)

    newObj = _dbWorker.Insert(cls, **kwargs)

    print('New return', newObj)
    return newObj


def Delete(obj):
    print('Delete(', obj)

    _dbWorker.Delete(obj)


def Drop(cls, confirm=False):
    print('Drop(', cls, confirm)
    if confirm:

        _dbWorker.Drop(cls)

    else:
        raise PermissionError('Cannot drop table "{}" unless you pass the kwarg "confirm=True".'.format(cls.__name__))

    print('Drop() return')


def FindOne(cls, **kwargs):
    print('FindOne(', cls, kwargs)
    findOneResult = _dbWorker.FindOne(cls, kwargs)
    print('FindOne return', findOneResult)
    return findOneResult


def FindAll(cls, **kwargs):
    print('FindAll(', cls, kwargs)
    findAllResult = _dbWorker.FindAll(cls, kwargs)

    print('FindAll return', findAllResult)
    return findAllResult


SetDebug(DEBUG)
