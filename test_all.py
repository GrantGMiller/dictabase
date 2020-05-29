import json
import time
import random
from dictabase import (
    RegisterDBURI,
    BaseTable,
    New,
    Delete,
    Drop,
    FindAll,
    FindOne,
)

RegisterDBURI()


def test_Simple():
    print('test_Simple(')

    class Simple(BaseTable):
        pass

    Drop(Simple, confirm=True)

    s = New(Simple, i=3888)
    print('s=', s)
    s['i'] = 99999
    print('s=', s)
    print('s reference gone')

    obj = FindOne(Simple)
    assert obj is not None
    assert obj['i'] == 99999

    foundObjs = list(FindAll(Simple))
    assert len(foundObjs) == 1

    for obj in foundObjs:
        print('obj=', obj)
        assert obj['i'] == 99999

    print('test_Simple() complete')


def test_Delete():
    class SimpleTestClass(BaseTable):
        pass

    print('Drop(SimpleTestClass, confirm=True)')
    Drop(SimpleTestClass, confirm=True)

    print('for i in range(10)')
    print('    New(SimpleTestClass, timeString=time.asctime(), count=i)')

    LENGTH = 10

    for i in range(LENGTH):
        New(SimpleTestClass, timeString=time.asctime(), count=i)

    print('FindAll(SimpleTestClass)=')
    allObjs = list(FindAll(SimpleTestClass))
    print('allObjs=', allObjs)
    assert len(allObjs) == LENGTH

    print('FindOne(SimpleTestClass, count=5)=')
    foundCount5 = list(FindAll(SimpleTestClass, count=5))
    print('foundCount5=', foundCount5)
    assert len(foundCount5) == 1

    print('for obj in FindAll(SimpleTestClass)')
    print('    obj["count"] += 10)')
    for obj in FindAll(SimpleTestClass):
        obj['count'] += 10

    print('FindAll(SimpleTestClass)=')
    for obj in list(FindAll(SimpleTestClass)):
        print('FindAll(obj=', obj)

    print('# Delete every-other')
    print('for i in range(0, 10, 2')
    print('    Delete(FindOne(SimpleTestClass, count=i+10')
    for i in range(0, 10, 2):
        obj = FindOne(SimpleTestClass, count=i + 10)
        if obj:
            print('Delete(obj=', obj)
            Delete(obj)

    print('FindAll(SimpleTestClass)=')
    allSimpleTestClass = list(FindAll(SimpleTestClass))
    print('len(allSimpleTestClass)=', len(allSimpleTestClass))
    for obj in allSimpleTestClass:
        print('allSimpleTestClass obj=', obj)
    assert len(allSimpleTestClass) == (LENGTH / 2)


def test_SimpleChild():
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
        newPerson = New(
            Person,
            name='Name{}'.format(i),
            age=30 + i,
        )
        print('newPerson=', newPerson)

        newAnimal = New(
            Animal,
            kind=random.choice(['Cat', 'Dog']),
            name='Fluffy{}'.format(i),
            age=i,
        )
        print('newAnimal=', newAnimal)

    # FindAll() returns all items from the database that match
    # you can also use keywords like '_limit', '_reverse', '_orderBy'
    assert len(list(FindAll(Animal, age=5))) == 1

    # FindOne() returns an newObj found in the database
    person5 = FindOne(Person, name='Name5')
    print('Age of Person5=', person5['age'])
    assert person5['age'] == 35


def test_ChildWithComplexKeys():
    # Test Relational Mapping

    class Book(BaseTable):
        def LoadKey(self, key, dbValue):
            # print('Book.LoadKey(', key, dbValue)
            return {
                'pages': lambda v: [FindOne(Page, id=obj['id']) for obj in json.loads(v)],
            }.get(key, lambda v: v)(dbValue)

        def DumpKey(self, key, objValue):
            # print('Book.DumpKey(', key, objValue, type(objValue))
            return {
                'pages': lambda listOfPages: json.dumps(
                    [dict(id=obj['id']) for obj in listOfPages],
                    sort_keys=True
                ),
            }.get(key, lambda v: v)(objValue)

    class Page(BaseTable):

        def LoadKey(self, key, dbValue):
            # print('Page.LoadKey(', key, dbValue)
            return {
                'book': lambda v: FindOne(Book, id=json.loads(v)['id'])
            }.get(key, lambda v: v)(dbValue)

        def DumpKey(self, key, objValue):
            # print('Page.DumpKey(', key, objValue, type(objValue))
            return {
                'book': lambda bookObj: json.dumps(dict(id=bookObj['id']))
            }.get(key, lambda v: v)(objValue)

    Drop(Book, confirm=True)
    Drop(Page, confirm=True)

    book = New(Book, title='MyTitle')
    page1 = New(Page, words='TheWords1')
    page2 = New(Page, words='TheWords2')

    print('77 book=', book)
    print('78 page1=', page1)
    print('79 page2=', page2)

    book['pages'] = [page1, page2]
    page1['book'] = book
    page2['book'] = book

    print('book["pages"]=', book['pages'])
    print('page1["book"]=', page1['book'])
    print('page2["book"]=', page2['book'])


def test_New():
    class Thingamajig(BaseTable):
        pass

    obj = New(Thingamajig, key1='value1')
    assert 'id' in obj
    print('New Thingamajig=', obj)


def test_None():
    class TableNone(BaseTable):
        pass

    obj = New(TableNone)
    obj['none'] = None

    foundObj = FindOne(TableNone, id=obj['id'])
    assert isinstance(foundObj['none'], type(None))


def test_ClassWithInitParams():
    class CustomizedInitClass(BaseTable):
        def __init__(self, string, integer, *a, **k):
            print("CustomizedInitClass.__init__(", string, integer, a, k)
            string = str(string)
            integer = int(integer)
            super().__init__(*a, string=string, integer=integer, **k)

    Drop(CustomizedInitClass, confirm=True)

    obj = New(CustomizedInitClass, string=12345, integer='98765')
    print('502 obj=', obj)

    foundObj = FindOne(CustomizedInitClass, id=obj['id'])
    print('503 foundObj=', foundObj)

    assert isinstance(foundObj['string'], str)
    assert isinstance(foundObj['integer'], int)


def test_Integers():
    class IntegerTable(BaseTable):
        pass

    Drop(IntegerTable, confirm=True)

    obj = New(IntegerTable, intOne=1, stringOne='1')
    print('525 obj=', obj)

    foundObj = FindOne(IntegerTable, id=obj['id'])
    print('528 foundObj=', foundObj)

    assert isinstance(foundObj['stringOne'], str)
    assert isinstance(foundObj['intOne'], int)


def test_MultipleInstances():
    class User(BaseTable):
        pass

    Drop(User, confirm=True)

    New(User, name='username1', age='33')

    userA = FindOne(User, name='username1')
    print('userA=', userA)
    userA['age'] = '99'

    userB = FindOne(User, name='username1')
    print('userB=', userB)
    userB['age'] = '00'

    for user in FindAll(User):
        print('user=', user)
        assert user['age'] == '00'


def test_MultipleSimultaneousInstances():
    class User(BaseTable):
        pass

    Drop(User, confirm=True)

    New(User, name='username1', age='33')

    userA = FindOne(User, name='username1')
    userB = FindOne(User, name='username1')

    print('userA=', userA)
    userA['age'] = '99'

    print('userB=', userB)
    userB['age'] = '00'

    print('userA=', userA)
    print('userB=', userB)

    for user in FindAll(User):
        print('user=', user)
        assert user['age'] == '00'


def test_Threading():
    from threading import Timer

    class Shape(BaseTable):
        pass

    Drop(Shape, confirm=True)

    def Function(name):
        print(f'Function({name})')

        New(Shape, name=name)

    LENGTH = 3
    for i in range(LENGTH):
        Timer(0, Function, (f'name{i}',)).start()

    count = 0
    while count < 10:
        # give the threads time to finish, but if they take too long, assume it falied
        allShapes = list(FindAll(Shape))
        print('allShapes=', allShapes)
        if len(allShapes) < LENGTH:
            count += 1
            time.sleep(1)
        else:
            break

    assert len(allShapes) == LENGTH
