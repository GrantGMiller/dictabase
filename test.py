import json
import datetime

if __name__ == '__main__':
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
        CommitAll,
    )

    RegisterDBURI(
        # None,  # use default sqllite
    )


    def TestSimple():
        class Simple(BaseTable):
            pass

        Drop(Simple, confirm=True)

        s = New(Simple, i=3888)
        print('s=', s)
        s['i'] = 99999
        print('s=', s)
        print('s reference gone')

        obj = FindOne(Simple)
        if obj is None or obj['i'] != 99999:
            raise Exception('should == 99999')

        for obj in FindAll(Simple):
            print('obj=', obj)
            if obj['i'] != 99999:
                raise Exception('obj["i"] should = 99999, got', obj['i'])



    def TestA():
        class A(BaseTable):
            pass

        print('Drop(A, confirm=True)')
        Drop(A, confirm=True)

        print('for i in range(10)')
        print('    New(A, timeString=time.asctime(), count=i)')
        for i in range(10):
            New(A, timeString=time.asctime(), count=i)

        print('FindAll(A)=')
        for obj in FindAll(A):
            print(obj)

        print('FindOne(A, count=5)=')
        for obj in FindAll(A, count=5):
            print(obj)

        print('for obj in FindAll(A)')
        print('    obj["count"] += 10)')
        for obj in FindAll(A):
            obj['count'] += 10

        print('FindAll(A)=')
        for obj in list(FindAll(A)):
            print(obj)

        print('# Delete every-other')
        print('for i in range(0, 10, 2')
        print('    Delete(FindOne(A, count=i+10')
        for i in range(0, 10, 2):
            obj = FindOne(A, count=i + 10)
            if obj:
                Delete(obj)

        print('FindAll(A)=')
        for obj in FindAll(A):
            print(obj)


    def TestBytes():
        # test bytes type
        class B(BaseTable):
            pass

        Drop(B, confirm=True)

        d = ('0' * 100).encode()
        try:
            large = New(B, data=d)
            failed = False
        except Exception as e:
            # should fail
            failed = True
            print('96', e)

        if not failed:
            raise Exception('should have failed')

        large = New(B, data=d.decode())
        print("large['data'] == baseTableObj is", large['data'] == d)

        largeID = large['id']

        findLarge = FindOne(B, id=largeID)
        print("findLarge['data'] == baseTableObj is", findLarge['data'].encode() == d)


    def TestSimpleChild():

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
                age=30+i,
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
        print('Number of animals of age 5: {}'.format(
            len(list(FindAll(Animal, age=5))))
        )
        if len(list(FindAll(Animal, age=5))) != 1:
            raise RuntimeError('Should be 1 and only 1 animal with age 5')

        # FindOne() returns an newObj found in the database
        person5 = FindOne(Person, name='Name5')
        print('Age of Person5=', person5['age'])
        if person5['age'] != 35:
            raise RuntimeError('person5 should be 35 years old')

        # Remove any animals with age >= 35
        for animal in FindAll(Animal):
            if animal['age'] >= 35:
                print('Removing animal=', animal)
                Delete(animal)

        print('Remaining Animals=', FindAll(Animal))

    def TestChildWithComplexKeys():
        # Test Relational Mapping

        class Book(BaseTable):
            def LoadKey(self, key, dbValue):
                print('Book.LoadKey(', key, dbValue)
                return {
                    'pages': lambda v: [FindOne(Page, id=obj['id']) for obj in json.loads(v)],
                }.get(key, lambda v: v)(dbValue)

            def DumpKey(self, key, objValue):
                print('Book.DumpKey(', key, objValue, type(objValue))
                return {
                    'pages': lambda listOfPages: json.dumps(
                        [dict(id=obj['id']) for obj in listOfPages],
                        sort_keys=True
                    ),
                }.get(key, lambda v: v)(objValue)

        class Page(BaseTable):

            def LoadKey(self, key, dbValue):
                print('Book.LoadKey(', key, dbValue)
                return {
                    'book': lambda v: FindOne(Book, id=json.loads(v)['id'])
                }.get(key, lambda v: v)(dbValue)

            def DumpKey(self, key, objValue):
                print('Book.DumpKey(', key, objValue, type(objValue))
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


    def TestNew():
        class Thingamajig(BaseTable):
            pass

        obj = New(Thingamajig, key1='value1')
        if 'id' not in obj:
            raise Exception('Should have returned a new ID')
        print('New Thingamajig=', obj)


    def TestNone():
        class TableNone(BaseTable):
            pass

        obj = New(TableNone)
        obj['none'] = None

        foundObj = FindOne(TableNone, id=obj['id'])
        if not isinstance(foundObj['none'], type(None)):
            raise TypeError('Should have returned NoneType')


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


    #################
    startTime = datetime.datetime.now()
    ##########

    TestSimple()
    TestChildWithComplexKeys()
    TestSimpleChild()
    TestA()
    TestBytes()
    TestNew()
    TestNone()
    TestClassWithInitParms()
    TestIntegers()

    #####################
    CommitAll()
    endTime = datetime.datetime.now()
    print('Test took', endTime - startTime)
