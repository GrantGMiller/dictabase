import dataset
import json

# The DB URI used to store/read all data. By default uses sqlite. You can change the DB by calling SetDB_URI(newName)
# This module supports any DBURI supported by SQLAlchemy
global DB_URI
DB_URI = 'sqlite:///MyDatabase.db'


def SetDB_URI(dburi):
    global DB_URI
    DB_URI = dburi


# Some types are not supported, use this list of types to jsonify the value when reading/writing
TYPE_CONVERT_TO_JSON = [list, dict]


def ConvertDictValuesToJson(dictObj):
    '''
    This is used to convert jsonify a value before storing it in the db
    :param dictObj:
    :return:
    '''
    for key, value in dictObj.copy().items():
        for aType in TYPE_CONVERT_TO_JSON:
            if isinstance(value, aType):
                try:
                    dictObj[key] = json.dumps(value)
                except:
                    pass
                break
    return dictObj


def ConvertDictJsonValuesToNative(dictObj):
    '''
    This is used to json.load the value when reconstrucing the object from the db
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


class _BaseDictabaseTable(dict):
    '''
    This class saves any changes to a database.
    You should subclass this class to create a new table.

    For example:
        class UserClass(_BaseDictabaseTable):
            uniqueKeys = ['email']

        user = UserClass(email='me@website.com', name='John')

        # Then later in your code you can call

        result = FindOne(UserClass(email='me@website.com')
        print('result=', result)
        >> user= UserClass(email='me@website.com', name='John')
    '''

    uniqueKeys = ['id']  # override this in your subclass to force a column to have unique values per row

    def AfterInsert(self, *args, **kwargs):
        '''
        Override this method to do something after object is inserted in database

        Example:

        class Post(_BaseDictabaseTable):

            def AfterInsert(self):
                self['insertionTimestamp'] = datetime.datetime.now()
        '''

    def CustomGetKey(self, key, value):
        '''
        This module relies on the types supported by the dataset package.
        If you have a custom type, you can use override this method to change the behavior of getting a value from the db

        Example:

        class Post(_BaseDictabaseTable):

            def CustomGetKey(self, key, value):
                if key == 'content':
                    return key, Markup(value) # cast the value as a flask.Markup object
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

        class MyCustomClass(_BaseDictabaseTable):
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
            # First check if there is already an object in database with the unique keys

            kwargs = ConvertDictValuesToJson(kwargs)

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
                        'Duplicate object. searchDict={}, kwargs={}, uniqueKeys={}, searchResults={}'.format(
                            searchDict,
                            kwargs,
                            self.uniqueKeys,
                            searchResults
                        ))

            # Create a new object and insert it in the database
            super().__init__(*args, **kwargs)
            InsertDB(self)

            # find the object we just created and get its 'id' from the database
            obj = FindOne(type(self), **self)
            self['id'] = obj['id']
            obj.AfterInsert()  # Call this so the programmer can specify actions after init

        else:
            # This is called by FindOne or FindAll to re-construct an object from the database
            dictObj = args[0]
            super().__init__(**dictObj)

    def _Save(self):
        '''
        Write the changes to the database
        :return:
        '''
        UpsertDB(self, self.uniqueKeys)

    def __setitem__(self, key, value):
        '''
        Any time a value is set to this object, the change will be updated in the database
        :param key:
        :param value:
        :return:
        '''
        key, value = self.CustomSetKey(key, value)

        for aType in TYPE_CONVERT_TO_JSON:
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
        superValue = super().get(*a, **k)
        try:
            value = json.loads(superValue)
            return value
        except Exception as err:
            print('92 err=', err, 'return', superValue)
            return superValue

    def __str__(self):
        '''

        :return: string like '<_BaseDictabaseTable: email=me@website.com, name=John>'
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


def InsertDB(obj):
    '''
    Add a new obj to the db
    :param obj: subclass of dict()
    :return:
    '''
    tableName = type(obj).__name__
    with dataset.connect(DB_URI) as DB:
        DB[tableName].insert(obj)
        DB.commit()


def UpsertDB(obj, listOfKeysThatMustMatch):
    '''
    Update or Insert the obj into the db
    :param obj: subclass of dict()
    :param listOfKeysThatMustMatch:
    :return:
    '''
    listOfKeysThatMustMatch += ['id']

    tableName = type(obj).__name__
    with dataset.connect(DB_URI) as DB:
        DB[tableName].upsert(obj, listOfKeysThatMustMatch)
        DB.commit()


def FindOne(objType, **k):
    '''
    Find an object in the db and return it
    :param objType:
    :param k:
    :return: None if no object found, or the obj itself

    Example:
    obj = FindOne(MyClass, name='grant')
    if obj is None:
        print('no object found')
    else:
        print('Found object=', obj)
    '''
    k = ConvertDictValuesToJson(k)

    dbName = objType.__name__

    with dataset.connect(DB_URI) as DB:

        ret = DB[dbName].find_one(**k)
        if ret is None:
            return None
        else:
            ret = objType(ret, doInsert=False)  # cast the return as its proper type
            return ret


def FindAll(objType, **k):
    '''
    Find all object in database that match the **k

    Also pass special kwargs to return objects in a certain order/limit

    FindAll(MyClass, _reverse=True) > returns all objects in reverse order

    FindAll(MyClass, _orderBy='Name') > returns all objects sorted by the "Name" column

    FindAll(MyClass, _limit=5) > return first 5 matching objects

    :param objType: type
    :param k: an empty dict like {} will return all items from table
    :return: a generator that will iterate thru all the results found, may have length 0
    '''

    reverse = k.pop('_reverse', False)  # bool

    orderBy = k.pop('_orderBy', None)  # str

    if reverse is True:
        if orderBy is not None:
            orderBy = '-' + orderBy
        else:
            orderBy = '-id'

    k = ConvertDictValuesToJson(k)
    dbName = objType.__name__
    with dataset.connect(DB_URI) as DB:
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
        return ret


def Drop(objType):
    '''
    Delete an entire table from the database
    :param objType:
    :return: None
    '''
    dbName = objType.__name__
    with dataset.connect(DB_URI) as DB:
        DB[dbName].drop()
        DB.commit()


def Delete(obj):
    '''
    Delete a row from the database
    :param obj: subclass of dict
    :return: None
    '''
    objType = type(obj)
    dbName = objType.__name__

    with dataset.connect(DB_URI) as DB:
        DB[dbName].delete(**obj)
        DB.commit()
