import json
import dataset
import sys
import datetime

DEBUG = True

oldPrint = print
if DEBUG is False:
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


DB_ALLOWABLE_TYPES = {
    int,
    float,
    type(None),
    str,
    bool,
    datetime.datetime,
    datetime.date,
    datetime.time,

}  # use set() to make "in" lookups faster


class RowHandler:
    def __init__(self, **kwargs):
        print('RowHandler.__init__(', kwargs)
        self._data = dict(**kwargs)  # should be a db safe dict
        if '_jsonKeys' in kwargs and kwargs['_jsonKeys']:
            self._jsonKeys = set(json.loads(kwargs['_jsonKeys']))
        else:
            self._jsonKeys = set()

    def _ConvertToDBSafe(self, key, value):
        print('RowHandler._ConvertToDBSafe(', key, value)
        if isinstance(value, RowHandler):
            value = {
                'id': value['id'],
                '_type': type(value).__name__,
            }
        elif isinstance(value, list):
            for index, subValue in enumerate(value.copy()):
                if isinstance(subValue, RowHandler):
                    value[index] = {
                        'id': subValue['id'],
                        '_type': type(subValue).__name__,
                    }

        if type(value) not in DB_ALLOWABLE_TYPES:
            print('json.dumps(', value)
            value = json.dumps(value)
            self._jsonKeys.add(key)  # store json'd values in db, so we can unjson them when they are retrieved

        print('RowHandler._ConvertToDBSafe() return', value, ', type=', type(value))
        return value

    def Set(self, key, value):
        print("BaseTable.Set(", key, value)

        if not isinstance(key, str):
            raise TypeError(f'key "{key}" should be of type str')
        elif isinstance(value, bytes):
            raise TypeError('use bytes.decode() to decode bytes before calling Set()')

        if 'id' in self._data:
            doInsert = False
        else:
            doInsert = True

        value = self._ConvertToDBSafe(key, value)
        self._data[key] = value

        if 'id' in self._data:
            self._doUpsert()
        elif doInsert:
            self._doInsert()

    def Get(self, key):
        print("BaseTable.Get(", key)
        value = self._data[key]
        if key in self._jsonKeys:
            value = json.loads(value)

        return value

    def Items(self):
        for key in self._data:
            yield key, self.Get(key)

    def Update(self, dict):
        for k, v in dict.items():
            self.Set(k, v)

    def _doUpsert(self):
        print('_doUpsert(self=', self)
        tableName = type(self).__name__
        _DB.begin()

        d = self._data.copy()
        d['_jsonKeys'] = json.dumps(list(self._jsonKeys))
        print('116 doUpsert d=', d)
        _DB[tableName].upsert(d, ['id'])  # find row with matching 'id' and update it

        _DB.commit()

    def _doInsert(self):
        print('_doInsert(self=', self)
        tableName = type(self).__name__
        _DB.begin()

        d = self._data.copy()
        d['_jsonKeys'] = json.dumps(list(self._jsonKeys))
        ID = _DB[tableName].insert(d)
        _DB.commit()
        self._data['id'] = ID

    def __str__(self):
        '''

        :return: string like '<BaseDictabaseTable: email=me@website.com, name=John>'
        '''
        itemsList = []
        for k, v, in self.Items():
            if k.startswith('_'):
                if DEBUG is False:
                    continue  # dont print these

            if isinstance(v, str) and len(v) > 25:
                v = v[:25] + '...'
            itemsList.append(('{}={}(type={})'.format(k, v, type(v).__name__)))

        return '<{}: {}>'.format(
            type(self).__name__,
            ', '.join(itemsList)
        )

    def __repr__(self):
        return str(self)

    def __iter__(self):
        for k in self._data:
            yield k


class BaseTable(RowHandler):
    def __setitem__(self, key, value):
        return super().Set(key, value)

    def __getitem__(self, key):
        return super().Get(key)

    def update(self, iterable):
        return super().Update(**iterable)

    def items(self):
        return super().Items()

    def get(self, key, default=None):
        if key in self._data:
            return super().Get(key)
        else:
            return default


def FindOneRow(cls, **k):
    # _DB.begin() # dont do this
    print('FindOne(cls=', cls, ', k=', k)
    dbName = cls.__name__
    tbl = _DB[dbName]
    ret = tbl.find_one(**k)

    if ret:
        ret = cls(**ret)
        print('FindOne return', ret)
        return ret
    else:
        print('FindOne return None')
        return None


def FindOne(cls, **k):
    return FindOneRow(cls, **k)


def FindAllRows(cls, **k):
    # special kwargs
    reverse = k.pop('_reverse', False)  # bool
    orderBy = k.pop('_orderBy', None)  # str
    if reverse is True:
        if orderBy is not None:
            orderBy = '-' + orderBy
        else:
            orderBy = '-id'

    # do look up
    dbName = cls.__name__
    if len(k) is 0:
        ret = _DB[dbName].all(order_by=[f'{orderBy}'])
    else:
        if orderBy is not None:
            ret = _DB[dbName].find(order_by=['{}'.format(orderBy)], **k)
        else:
            ret = _DB[dbName].find(**k)

    # yield type-cast items one by one
    for d in ret:
        obj = cls(**d)
        yield obj


def FindAll(cls, **kwargs):
    return FindAllRows(cls, **kwargs)


def Drop(cls, confirm=False):
    global _DB
    if confirm:
        count = 0
        while count < 5:
            try:
                _DB.begin()
                tableName = cls.__name__
                _DB[tableName].drop()
                _DB.commit()

                # _DB = dataset.connect(_DBURI)
                break
            except:
                pass
            count += 1
    else:
        raise Exception('Cannot drop unless you pass confirm=True as kwarg')


def Delete(obj):
    print('Delete(', obj)
    _DB.begin()
    dbName = type(obj).__name__
    _DB[dbName].delete(**dict(obj.Items()))
    _DB.commit()


def NewRow(cls, **kwargs):
    '''
    Creates a new row in the table(cls)
    Returns the new dict-like object

    cls should inherit from BaseTable
    '''
    print('New(cls=', cls, ', kwargs=', kwargs)

    newObj = cls(**kwargs)
    for k, v in kwargs.items():
        newObj.Set(k, v)

    return newObj


def New(cls, **kwargs):
    return NewRow(cls, **kwargs)


#################################################################
if __name__ == '__main__':
    import time
    import random

    RegisterDBURI(
        # 'postgres://xfgkxpzruxledr:5b83aece4fbad7827cb1d9df48bf5b9c9ad2b33538662308a9ef1d8701bfda4b@ec2-35-174-88-65.compute-1.amazonaws.com:5432/d8832su8tgbh82'
        None,  # use default sqllite
    )


    def TestA():
        class A(BaseTable):
            pass

        Drop(A, confirm=True)

        for i in range(10):
            New(A, timeString=time.asctime(), count=i)

        oldPrint('FindAll(A)=', list(FindAll(A)))
        oldPrint('FindOne(A, count=5)=', FindOne(A, count=5))

        for item in FindAll(A):
            item['count'] += 10

        oldPrint('FindAll(A)=', list(FindAll(A)))

        for i in range(0, 10, 2):
            obj = FindOne(A, count=i + 10)
            if obj:
                Delete(obj)

        oldPrint('FindAll(A)=', list(FindAll(A)))


    def TestBytes():
        # test bytes type
        class B(BaseTable):
            pass

        Drop(B, confirm=True)

        d = ('0' * 100).encode()
        try:
            large = New(B, data=d)

        except Exception as e:
            oldPrint(e)
        large = New(B, data=d.decode())
        oldPrint("large['data'] == baseTableObj is", large['data'] == d)

        largeID = large['id']

        findLarge = FindOne(B, id=largeID)
        oldPrint("findLarge['data'] == baseTableObj is", findLarge['data'].encode() == d)


    def TestTypes():

        class Person(BaseTable):
            # Each subclass of BaseTable produces another table in the db
            pass

        class Animal(BaseTable):
            pass

        # For testing, delete all tables first
        # Comment these out to make data persistant
        Drop(Person, confirm=True)
        Drop(Animal, confirm=True)

        # Create tables with random data
        for i in range(10):
            # Instantiating a new Person newObj adds a new row in the db
            newPerson = New(Person,
                            name='Name{}'.format(i),
                            age=random.randint(1, 100),
                            )
            oldPrint('newPerson=', newPerson)

            newAnimal = New(Animal,
                            kind=random.choice(['Cat', 'Dog']),
                            name='Fluffy{}'.format(i),
                            age=random.randint(1, 10),
                            )
            oldPrint('newAnimal=', newAnimal)

        # FindAll() returns all items from the database that match
        # you can also use keywords like '_limit', '_reverse', '_orderBy'
        oldPrint('Number of animals of age 5: {}'.format(
            len(list(FindAll(Animal, age=5))))
        )

        # FindOne() returns an newObj found in the database
        person5 = FindOne(Person, name='Name5')
        oldPrint('Age of Person5=', person5['age'])

        # Remove any animals with age >= 5
        for animal in FindAll(Animal):
            if animal['age'] >= 5:
                oldPrint('Removing animal=', animal)
                Delete(animal)

        oldPrint('Remaining Animals=', FindAll(Animal))

        # Test Relational Mapping

        class Book(BaseTable):
            pass

        class Page(BaseTable):
            pass

        Drop(Book, confirm=True)
        Drop(Page, confirm=True)

        book = New(Book, title='Title')
        page1 = New(Page, words='Words1')
        page2 = New(Page, words='Words2')

        oldPrint('77 book=', book)
        oldPrint('78 page1=', page1)
        oldPrint('79 page2=', page2)

        book['pages'] = [page1, page2]
        page1['book'] = book
        page2['book'] = book

        oldPrint('book["pages"]=', book['pages'])
        oldPrint('page1["book"]=', page1['book'])
        oldPrint('page2["book"]=', page2['book'])


    def TestList():
        class TestListTable(BaseTable):
            pass

        Drop(TestListTable, confirm=True)

        item = New(TestListTable)
        item['list'] = [1, 2, 3]
        print(411, 'item=', item)

        findItem = FindOne(TestListTable)
        oldPrint('findItem=', findItem)

        for k, v in findItem.items():
            oldPrint(327, k, '=', v, 'type=', type(v))
            if k == 'list':
                if not isinstance(v, list):
                    raise TypeError('Should be type list')

        for k in findItem:
            v = findItem.get(k)
            oldPrint(319, k, '=', v, 'type=', type(v))
            if k == 'list':
                if not isinstance(v, list):
                    raise TypeError('Should be type list')

        for k in findItem:
            v = findItem[k]
            oldPrint(325, k, '=', v, 'type=', type(v))
            if k == 'list':
                if not isinstance(v, list):
                    raise TypeError('Should be type list')

        # test list of list
        newObj = New(TestListTable)
        newObj['listOfList'] = [[i for i in range(3)] for i in range(5, 10)]
        print('367 newObj=', newObj)

        foundObj = FindOne(TestListTable, id=newObj['id'])
        print('370 foundObj=', foundObj)
        if not isinstance(foundObj['listOfList'], list):
            raise TypeError('Should be type list')

        if not isinstance(foundObj['listOfList'][0], list):
            raise TypeError('Should be type list')

        # test list of list of strings
        l = [[str(i) for i in range(3)] for i in range(5, 10)]
        newObj = New(TestListTable, listOfListOfStrings=l)
        print('380 newObj=', newObj)

        foundObj = FindOne(TestListTable, id=newObj['id'])
        print('383 foundObj=', foundObj)
        if not isinstance(foundObj['listOfListOfStrings'], list):
            raise TypeError('Should be type list')

        print("foundObj['listOfListOfStrings'][0]=", foundObj['listOfListOfStrings'][0])
        print('type=', type(foundObj['listOfListOfStrings'][0][0]))
        if not isinstance(foundObj['listOfListOfStrings'][0][0], str):
            raise TypeError('Should be type list')


    def TestNew():
        class Thingamajig(BaseTable):
            pass

        obj = New(Thingamajig, key1='value1')
        if 'id' not in obj:
            raise Exception('Should have returned a new ID')
        oldPrint('New Thingamajig=', obj)


    def TestDict():
        class TestDictTable(BaseTable):
            pass

        Drop(TestDictTable, confirm=True)
        item = New(TestDictTable)
        item['dict'] = {1: 'one', '2': 'two', 'three': 3, 'four': '4'}

        findItem = FindOne(TestDictTable, id=item['id'])
        oldPrint('findItem=', findItem)

        for k, v in findItem.items():
            oldPrint(327, k, '=', v, 'type=', type(v))
            if k == 'dict':
                if not isinstance(v, dict):
                    raise TypeError('Should be type dict')

        for k in findItem:
            v = findItem.get(k)
            oldPrint(319, k, '=', v, 'type=', type(v))
            if k == 'dict':
                if not isinstance(v, dict):
                    raise TypeError('Should be type dict')

        for k in findItem:
            v = findItem[k]
            oldPrint(325, k, '=', v, 'type=', type(v))
            if k == 'dict':
                if not isinstance(v, dict):
                    raise TypeError('Should be type dict')


    def TestNone():
        class TableNone(BaseTable):
            pass

        obj = New(TableNone)
        obj['none'] = None

        foundObj = FindOne(TableNone, id=obj['id'])
        if not isinstance(foundObj['none'], type(None)):
            raise TypeError('Should have returned NoneType')


    def TestJsonableInNew():
        class JsonalbeTest(BaseTable):
            pass

        Drop(JsonalbeTest, confirm=True)

        obj1 = New(JsonalbeTest)
        obj1['l'] = [1, 2, 3, [4, 5, 6]]
        print('obj1=', obj1)

        foundObj1 = FindOne(JsonalbeTest, id=obj1['id'])
        print('foundObj1=', foundObj1)

        obj2 = New(JsonalbeTest, l=[1, 2, 3, [4, 5, 6]])
        print('obj2=', obj2)

        foundObj2 = FindOne(JsonalbeTest, id=obj2['id'])
        print('foundObj2=', foundObj2)


    def TestClassWithInitParms():
        class CustomizedInitClass(BaseTable):
            def __init__(self, string, integer, *a, **k):
                print("CustomizedInitClass.__init__(", string, integer, a, k)
                string = str(string)
                integer = int(integer)
                super().__init__(string=string, integer=integer)

        obj = New(CustomizedInitClass, string=12345, integer='98765')
        print('502 obj=', obj)

        foundObj = FindOne(CustomizedInitClass, id=obj['id'])
        print('503 foundObj=', foundObj)

        if not isinstance(foundObj['string'], str):
            raise TypeError('"string" should be str')

        if not isinstance(foundObj['integer'], int):
            raise TypeError('"integer" should be int')


    def TestIntegers():
        class IntegerTable(BaseTable):
            pass

        obj = New(IntegerTable, intOne=1, stringOne='1')
        print('525 obj=', obj)

        foundObj = FindOne(IntegerTable, id=obj['id'])
        print('528 foundObj=', foundObj)

        if not isinstance(foundObj['intOne'], int):
            raise TypeError('"intOne" should be int')

        if not isinstance(foundObj['stringOne'], str):
            raise TypeError('"stringOne" should be str')


    def TestRowHandlerFunctions():
        class TestRowHandler(RowHandler):
            pass

        obj = NewRow(TestRowHandler)
        obj.Set('string', 'myValue')
        obj.Set('integer', 1)
        obj.Set('bytes', b'\x00\x012345'.decode())
        obj.Set('list', [i for i in range(5)])
        print('213 obj=', obj)

        foundObj = FindOneRow(TestRowHandler, id=obj.Get('id'))
        print('216 foundObj=', foundObj)

        fondRows = FindAllRows(TestRowHandler)
        print('234 fondRows=', list(fondRows))


    #################
    TestRowHandlerFunctions()
    TestA()
    TestBytes()
    TestTypes()
    TestList()
    TestNew()
    TestDict()
    TestNone()
    TestJsonableInNew()
    TestClassWithInitParms()
    TestIntegers()
