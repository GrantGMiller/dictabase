if __name__ == '__main__':
    import time
    import random
    from dictabase3 import (
        RegisterDBURI,
        BaseTable,
        New,
        Delete,
        Drop,
        FindAll,
        FindOne,
    )

    RegisterDBURI(
        # 'postgres://xfgkxpzruxledr:5b83aece4fbad7827cb1d9df48bf5b9c9ad2b33538662308a9ef1d8701bfda4b@ec2-35-174-88-65.compute-1.amazonaws.com:5432/d8832su8tgbh82'
        None,  # use default sqllite
    )


    def TestSimple():
        class Simple(BaseTable):
            pass

        Drop(Simple, confirm=True)

        s = New(Simple, i=random.randint(0, 100))
        print('s=', s)
        s['i'] = 'override'
        print('s=', s)
        print('s reference gone')
        for obj in FindAll(Simple):
            print('obj=', obj)
            if obj['i'] != 'override':
                raise Exception('obj["i"] should = "override"')


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

        except Exception as e:
            print(e)
        large = New(B, data=d.decode())
        print("large['data'] == baseTableObj is", large['data'] == d)

        largeID = large['id']

        findLarge = FindOne(B, id=largeID)
        print("findLarge['data'] == baseTableObj is", findLarge['data'].encode() == d)


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
            print('newPerson=', newPerson)

            newAnimal = New(Animal,
                            kind=random.choice(['Cat', 'Dog']),
                            name='Fluffy{}'.format(i),
                            age=random.randint(1, 10),
                            )
            print('newAnimal=', newAnimal)

        # FindAll() returns all items from the database that match
        # you can also use keywords like '_limit', '_reverse', '_orderBy'
        print('Number of animals of age 5: {}'.format(
            len(list(FindAll(Animal, age=5))))
        )

        # FindOne() returns an newObj found in the database
        person5 = FindOne(Person, name='Name5')
        print('Age of Person5=', person5['age'])

        # Remove any animals with age >= 5
        for animal in FindAll(Animal):
            if animal['age'] >= 5:
                print('Removing animal=', animal)
                Delete(animal)

        print('Remaining Animals=', FindAll(Animal))

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

        print('77 book=', book)
        print('78 page1=', page1)
        print('79 page2=', page2)

        book['pages'] = [page1, page2]
        page1['book'] = book
        page2['book'] = book

        print('book["pages"]=', book['pages'])
        print('page1["book"]=', page1['book'])
        print('page2["book"]=', page2['book'])


    def TestList():
        class TestListTable(BaseTable):
            pass

        Drop(TestListTable, confirm=True)

        item = New(TestListTable)
        item['list'] = [1, 2, 3]
        print(411, 'item=', item)

        findItem = FindOne(TestListTable)
        print('findItem=', findItem)

        for k, v in findItem.items():
            print(327, k, '=', v, 'type=', type(v))
            if k == 'list':
                if not isinstance(v, list):
                    raise TypeError('Should be type list')

        for k in findItem:
            v = findItem.get(k)
            print(319, k, '=', v, 'type=', type(v))
            if k == 'list':
                if not isinstance(v, list):
                    raise TypeError('Should be type list')

        for k in findItem:
            v = findItem[k]
            print(325, k, '=', v, 'type=', type(v))
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
        print('New Thingamajig=', obj)


    def TestDict():
        class TestDictTable(BaseTable):
            pass

        Drop(TestDictTable, confirm=True)
        item = New(TestDictTable)
        item['dict'] = {1: 'one', '2': 'two', 'three': 3, 'four': '4'}

        findItem = FindOne(TestDictTable, id=item['id'])
        print('findItem=', findItem)

        for k, v in findItem.items():
            print(327, k, '=', v, 'type=', type(v))
            if k == 'dict':
                if not isinstance(v, dict):
                    raise TypeError('Should be type dict')

        for k in findItem:
            v = findItem.get(k)
            print(319, k, '=', v, 'type=', type(v))
            if k == 'dict':
                if not isinstance(v, dict):
                    raise TypeError('Should be type dict')

        for k in findItem:
            v = findItem[k]
            print(325, k, '=', v, 'type=', type(v))
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


    #################
    TestSimple()
    # TestA()
    # TestBytes()
    # TestTypes()
    # TestList()
    # TestNew()
    # TestDict()
    # TestNone()
    # TestJsonableInNew()
    # TestClassWithInitParms()
    # TestIntegers()
