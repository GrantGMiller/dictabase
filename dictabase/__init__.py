import dataset
import json
from queue import Queue
from threading import Timer
import time

# The DB URI used to store/read all data. By default uses sqlite. You can change the DB by calling SetDB_URI(newName)
# This module supports any DBURI supported by SQLAlchemy
global _DB_URI
_DB_URI = 'sqlite:///MyDatabase.db'
global _DB
_DB = None


def SetDB_URI(dburi=None):
    '''
    Set the URI for the database.
    Supports any URI supported by SQLAlchemy. Defaults to sqllite
    :param dburi: str like 'sqlite:///MyDatabase.db'
    :return:
    '''
    global _DB_URI
    global _DB
    dburi = dburi or 'sqlite:///MyDatabase.db'
    _DB_URI = dburi
    _DB = dataset.connect(_DB_URI)


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
    This is used to json.load the value when reconstrucing the newObj from the db
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
        Override this method to do something after newObj is inserted in database

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
                    return key, Markup(value) # cast the value as a flask.Markup newObj
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
            # First check if there is already an newObj in database with the unique keys

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
                        'Duplicate newObj. searchDict={}, kwargs={}, uniqueKeys={}, searchResults={}'.format(
                            searchDict,
                            kwargs,
                            self.uniqueKeys,
                            searchResults
                        ))

            # Create a new newObj and insert it in the database
            super().__init__(*args, **kwargs)
            obj = _InsertDB(self)
            while obj is None:
                time.sleep(1)
                print('178 newObj=', obj)

            # self['id'] = newObj['id']  # i think this is causing a threading error
            super().__setitem__('id', obj['id'])

            self.AfterInsert()  # Call this so the programmer can specify actions after init

        else:
            # This is called by FindOne or FindAll to re-construct an newObj from the database
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
        Any time a value is set to this newObj, the change will be updated in the database
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
    '''
    Add a new newObj to the db
    :param obj: subclass of dict()
    :return:
    '''
    global _DB
    if _DB is None:
        SetDB_URI()

    tableName = type(obj).__name__


    _DB[tableName].insert(obj)
    _DB.commit()

    ret = FindOne(type(obj), **obj)
    while ret is None:
        print('not found, trying again', ret, obj, 'all=', FindAll(type(obj), **obj))
        time.sleep(1)
        ret = FindOne(type(obj), **obj)

    return ret


def _UpsertDB(obj, listOfKeysThatMustMatch):
    _DoUpsertDB(obj, listOfKeysThatMustMatch)


def _DoUpsertDB(newObj, listOfKeysThatMustMatch):
    '''
    Update and/or Insert the obj into the db
    :param newObj: subclass of dict()
    :param listOfKeysThatMustMatch: list of str
    :return:
    '''
    global _DB
    if _DB is None:
        SetDB_URI()


    listOfKeysThatMustMatch += ['id']
    listOfKeysThatMustMatch = list(set(listOfKeysThatMustMatch))  # remove duplicates

    newType = type(newObj)

    oldObj = FindOne(newType, id=dict(newObj)['id'])
    oldObj.update(newObj)

    upsertObj = oldObj

    tableName = type(upsertObj).__name__
    _DB[tableName].upsert(upsertObj, listOfKeysThatMustMatch)
    _DB.commit()


def FindOne(objType, **k):
    '''
    Find an newObj in the db and return it
    :param objType:
    :param k:
    :return: None if no newObj found, or the newObj itself

    Example:
    newObj = FindOne(MyClass, name='grant')
    if newObj is None:
        print('no newObj found')
    else:
        print('Found newObj=', newObj)
    '''
    global _DB
    if _DB is None:
        SetDB_URI()


    k = _ConvertDictValuesToJson(k)

    dbName = objType.__name__

    ret = _DB[dbName].find_one(**k)
    if ret is None:
        ret = None
    else:
        ret = objType(ret, doInsert=False)  # cast the return as its proper type

    return ret


def FindAll(objType, **k):
    '''
    Find all newObj in database that match the **k

    Also pass special kwargs to return objects in a certain order/limit

    FindAll(MyClass, _reverse=True) > returns all objects in reverse order

    FindAll(MyClass, _orderBy='Name') > returns all objects sorted by the "Name" column

    FindAll(MyClass, _limit=5) > return first 5 matching objects

    :param objType: type
    :param k: an empty dict like {} will return all items from table
    :return: a generator that will iterate thru all the results found, may have length 0
    '''
    global _DB
    if _DB is None:
        SetDB_URI()


    reverse = k.pop('_reverse', False)  # bool

    orderBy = k.pop('_orderBy', None)  # str

    if reverse is True:
        if orderBy is not None:
            orderBy = '-' + orderBy
        else:
            orderBy = '-id'

    k = _ConvertDictValuesToJson(k)
    dbName = objType.__name__

    if len(k) is 0:
        if orderBy is not None:
            ret = _DB[dbName].all(order_by=['{}'.format(orderBy)])
        else:
            ret = _DB[dbName].all()

    else:

        if orderBy is not None:
            ret = _DB[dbName].find(order_by=['{}'.format(orderBy)], **k)
        else:
            ret = _DB[dbName].find(**k)

    ret = [objType(item, doInsert=False) for item in list(ret)]

    return ret


def Drop(objType, confirm=False):
    if confirm:
        _DoDrop(objType)
    else:
        raise Exception('Cannot drop unless you pass confirm=True as kwarg')


def _DoDrop(objType):
    '''
    Delete an entire table from the database

    :param objType:
    :return: None
    '''
    global _DB
    if _DB is None:
        SetDB_URI()

    dbName = objType.__name__
    _DB[dbName].drop()
    _DB.commit()


def Delete(obj):
    _DoDelete(obj)


def _DoDelete(obj):
    '''
    Delete a row from the database

    :param obj: subclass of dict
    :return: None
    '''
    global _DB
    if _DB is None:
        SetDB_URI()

    obj = FindOne(type(obj), id=obj['id'])

    objType = type(obj)
    dbName = objType.__name__

    _DB[dbName].delete(**obj)
    _DB.commit()


if __name__ == '__main__':
    import time


    class A(BaseDictabaseTable):
        pass


    a = A(time=time.asctime())
    print('a=', a)
    print('FindAll=', FindAll(A))
