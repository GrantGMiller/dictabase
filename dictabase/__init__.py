import dataset
import json
from queue import Queue
from threading import Timer

# The DB URI used to store/read all data. By default uses sqlite. You can change the DB by calling SetDB_URI(newName)
# This module supports any DBURI supported by SQLAlchemy
global _DB_URI
_DB_URI = 'sqlite:///MyDatabase.db'


def SetDB_URI(dburi):
    '''
    Set the URI for the database.
    Supports any URI supported by SQLAlchemy. Defaults to sqllite
    :param dburi: str like 'sqlite:///MyDatabase.db'
    :return:
    '''
    global _DB_URI
    _DB_URI = dburi


# Some types are not supported, use this list of types to jsonify the value when reading/writing
_TYPE_CONVERT_TO_JSON = [list, dict]


def _ConvertDictValuesToJson(dictObj):
    '''
    This is used to convert jsonify a value before storing it in the db
    :param dictObj:
    :return:
    '''
    for key, value in dictObj.copy().items():
        for aType in _TYPE_CONVERT_TO_JSON:
            if isinstance(value, aType):
                try:
                    dictObj[key] = json.dumps(value)
                except:
                    pass
                break
    return dictObj


def _ConvertDictJsonValuesToNative(dictObj):
    '''
    This is used to json.load the value when reconstrucing the obj from the db
    :param dictObj:
    :return:
    '''
    for key, value in dictObj.copy().items():
        try:
            newValue = json.loads(value)
            dictObj[key] = newValue
        except:
            pass
    return dictObj


class BaseDictabaseTable(dict):
    '''
    This class saves any changes to a database.
    You should subclass this class to create a new table.

    For example:
        class UserClass(BaseDictabaseTable):
            pass

        user = UserClass(email='me@website.com', name='John')

        # Then later in your code you can call

        result = FindOne(UserClass, email='me@website.com')
        print('result=', result)
        >> result= UserClass(email='me@website.com', name='John')
    '''

    uniqueKeys = ['id']  # override this in your subclass to force a column to have unique values per row

    def AfterInsert(self, *args, **kwargs):
        '''
        Override this method to do something after obj is inserted in database

        Example:

        class Post(BaseDictabaseTable):

            def AfterInsert(self):
                self['insertionTimestamp'] = datetime.datetime.now()
        '''

    def CustomGetKey(self, key, value):
        '''
        This module relies on the types supported by the dataset package.
        If you have a custom type, you can use override this method to change the behavior of getting a value from the db

        Example:

        class Post(BaseDictabaseTable):

            def CustomGetKey(self, key, value):
                if key == 'content':
                    return key, Markup(value) # cast the value as a flask.Markup obj
                else:
                    return key, value # return the value normally

        '''
        return key, value

    def CustomSetKey(self, key, value):
        '''
        This module relies on the types supported by the dataset package.
        If you have a custom type, you can use this method to convert it to a supported type before writing it to the db

        :param key:
        :param value:
        :return: tuple of (newKey, newValue)

        Example:

        class MyCustomClass(BaseDictabaseTable):
            def CustomSetKey(self, key, value):
                if key == 'CustomKeyNotSupportedByDataSet':
                    value = dict(value)
                    return key, value
                else:
                    return key, value
        '''
        return key, value

    def __init__(self, *args, **kwargs):
        '''
        Accepts same args/kwargs as normal python dict

        :param args:
        :param kwargs:
        '''

        doInsert = kwargs.pop('doInsert', True)
        if doInsert is True:
            # First check if there is already an obj in database with the unique keys

            kwargs = _ConvertDictValuesToJson(kwargs)

            searchDict = dict()
            for key in self.uniqueKeys:
                if key in kwargs:
                    searchDict[key] = kwargs[key]

            if len(searchDict) > 0:
                # check for duplicate rows in the db
                searchResults = FindAll(type(self), **searchDict)

                duplicateExists = False
                for item in searchResults:
                    duplicateExists = True
                    # if len(searchResults) is 0, this wont happen and duplicateExists == False
                    break

                if duplicateExists:
                    raise Exception(
                        'Duplicate obj. searchDict={}, kwargs={}, uniqueKeys={}, searchResults={}'.format(
                            searchDict,
                            kwargs,
                            self.uniqueKeys,
                            searchResults
                        ))

            # Create a new obj and insert it in the database
            super().__init__(*args, **kwargs)
            obj = _DoInsertDB(self)

            print('178 obj=', obj)
            # self['id'] = obj['id']  # i think this is causing a threading error
            super().__setitem__('id', obj['id'])

            self.AfterInsert()  # Call this so the programmer can specify actions after init

        else:
            # This is called by FindOne or FindAll to re-construct an obj from the database
            dictObj = args[0]
            super().__init__(**dictObj)

    def _Save(self):
        '''
        Write the changes to the database
        :return:
        '''
        _UpsertDB(self, self.uniqueKeys)

    def __setitem__(self, key, value):
        '''
        Any time a value is set to this obj, the change will be updated in the database
        :param key:
        :param value:
        :return:
        '''
        key, value = self.CustomSetKey(key, value)

        for aType in _TYPE_CONVERT_TO_JSON:
            if isinstance(value, aType):
                value = json.dumps(value)
                break

        super().__setitem__(key, value)
        self._Save()

    def __getitem__(self, key):
        superValue = super().__getitem__(key)
        try:
            value = json.loads(superValue)
            ret = value
        except Exception as err:
            ret = superValue

        _, ret = self.CustomGetKey(key, ret)
        return ret

    def get(self, *a, **k):
        '''
        Works the same as the built in python dict.get
        :param a:
        :param k:
        :return:
        '''
        superValue = super().get(*a, **k)
        try:
            value = json.loads(superValue)
            return value
        except Exception as err:
            print('92 err=', err, 'return', superValue)
            return superValue

    def __str__(self):
        '''

        :return: string like '<BaseDictabaseTable: email=me@website.com, name=John>'
        '''
        itemsList = []
        for k, v, in self.items():
            try:
                itemsList.append(('{}={}'.format(k, v.encode())))
            except:
                itemsList.append(('{}={}'.format(k, v)))

        return '<{}: {}>'.format(
            type(self).__name__,
            ', '.join(itemsList)
        )

    def __repr__(self):
        return str(self)


def _InsertDB(obj):
    queueManager.Add('insert', obj)


def _DoInsertDB(obj):
    '''
    Add a new obj to the db
    :param obj: subclass of dict()
    :return:
    '''
    tableName = type(obj).__name__
    with dataset.connect(_DB_URI) as DB:
        DB[tableName].insert(obj)
        DB.commit()

    return FindOne(type(obj), **obj)


def _UpsertDB(obj, listOfKeysThatMustMatch):
    queueManager.Add('upsert', obj, listOfKeysThatMustMatch)


def _DoUpsertDB(obj, listOfKeysThatMustMatch):
    '''
    Update or Insert the obj into the db
    :param obj: subclass of dict()
    :param listOfKeysThatMustMatch:
    :return:
    '''
    listOfKeysThatMustMatch += ['id']

    tableName = type(obj).__name__
    with dataset.connect(_DB_URI) as DB:
        DB[tableName].upsert(obj, listOfKeysThatMustMatch)
        DB.commit()


def FindOne(objType, **k):
    '''
    Find an obj in the db and return it
    :param objType:
    :param k:
    :return: None if no obj found, or the obj itself

    Example:
    obj = FindOne(MyClass, name='grant')
    if obj is None:
        print('no obj found')
    else:
        print('Found obj=', obj)
    '''
    queueManager.Pause()

    k = _ConvertDictValuesToJson(k)

    dbName = objType.__name__

    with dataset.connect(_DB_URI) as DB:

        ret = DB[dbName].find_one(**k)
        if ret is None:
            ret = None
        else:
            ret = objType(ret, doInsert=False)  # cast the return as its proper type

    queueManager.Resume()
    return ret


def FindAll(objType, **k):
    '''
    Find all obj in database that match the **k

    Also pass special kwargs to return objects in a certain order/limit

    FindAll(MyClass, _reverse=True) > returns all objects in reverse order

    FindAll(MyClass, _orderBy='Name') > returns all objects sorted by the "Name" column

    FindAll(MyClass, _limit=5) > return first 5 matching objects

    :param objType: type
    :param k: an empty dict like {} will return all items from table
    :return: a generator that will iterate thru all the results found, may have length 0
    '''
    queueManager.Pause()

    reverse = k.pop('_reverse', False)  # bool

    orderBy = k.pop('_orderBy', None)  # str

    if reverse is True:
        if orderBy is not None:
            orderBy = '-' + orderBy
        else:
            orderBy = '-id'

    k = _ConvertDictValuesToJson(k)
    dbName = objType.__name__
    with dataset.connect(_DB_URI) as DB:
        if len(k) is 0:
            if orderBy is not None:
                ret = DB[dbName].all(order_by=['{}'.format(orderBy)])
            else:
                ret = DB[dbName].all()

        else:

            if orderBy is not None:
                ret = DB[dbName].find(order_by=['{}'.format(orderBy)], **k)
            else:
                ret = DB[dbName].find(**k)

        ret = [objType(item, doInsert=False) for item in list(ret)]

    queueManager.Resume()
    return ret


def Drop(objType):
    # queueManager.Add('drop', objType)
    queueManager.Pause()
    _DoDrop(objType)
    queueManager.Resume()


def _DoDrop(objType):
    '''
    Delete an entire table from the database

    :param objType:
    :return: None
    '''
    dbName = objType.__name__
    with dataset.connect(_DB_URI) as DB:
        DB[dbName].drop()
        DB.commit()


def Delete(obj):
    # queueManager.Add('delete', obj)
    queueManager.Pause()
    _DoDelete(obj)
    queueManager.Resume()


def _DoDelete(obj):
    '''
    Delete a row from the database

    :param obj: subclass of dict
    :return: None
    '''
    objType = type(obj)
    dbName = objType.__name__

    with dataset.connect(_DB_URI) as DB:
        DB[dbName].delete(**obj)
        DB.commit()


class QueueManager:
    def __init__(self):
        self._q = Queue()
        self._timer = None
        self._pause = False

    def Pause(self):
        self._pause = True
        if self._timer and self._timer.isAlive():
            self._timer.cancel()

    def Resume(self):
        if self._timer is None:
            self._timer = Timer(0, self._ProcessOneQueueItem)
            self._timer.start()

    def Add(self, command, *args, **kwargs):
        self._q.put((command, args, kwargs))

        self.Resume()

    def _ProcessOneQueueItem(self):
        command, args, kwargs = self._q.get()

        func = {
            'upsert': _DoUpsertDB,
            'insert': _DoInsertDB,
            'drop': _DoDrop,
            'delete': _DoDelete
        }.get(command)
        try:
            func(*args, **kwargs)
        except Exception as e:
            print('441 Exception:', func, args, kwargs, '\r\n', e)

        self._q.task_done()
        if self._q.empty():
            self._timer = None
        else:
            self._timer = Timer(0, self._ProcessOneQueueItem)
            self._timer.start()


queueManager = QueueManager()

if __name__ == '__main__':
    import time


    class A(BaseDictabaseTable):
        pass


    a = A(time=time.asctime())
    print('a=', a)
    print('FindAll=', FindAll(A))
    input()
