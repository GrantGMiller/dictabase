A database interface that acts like a python dictionary

Install
=======
pip install dictabase


Create a new table
------------------

::

    # Create a table representing users
    from dictabase import BaseTable, RegisterDBURI

    RegisterDBURI() # pass nothing to default to sqlite

    class UserClass(BaseTable):
        pass

    newUser = New(UserClass, name='Grant', age=31)
    newUser = New(UserClass, name='Bob', age=99)
    # there is now a sqlite database containing the 2 users. Yup, thats it!

Look up items in the database
-----------------------------

::

    from dictabase import FindOne, FindAll

    allUsers = FindAll(UserClass)
    # allUsers is an iterable of all the UserClass objects that exists in the database
    print('allUsers=', list(allUsers))
    >> allUsers= [<UserClass: name='Grant', age=31>, <UserClass: name='Bob', age=99>]

    user = FindOne(UserClass, name='Grant')
    print('user=', user)
    >>user= <UserClass: name='Grant', age=31>

    user = FindOne(UserClass, name='NotARealName')
    print('user=', user)
    >>user= None

Read/Write to the database
--------------------------

::

    # find an object that was previously put in the database
    user = FindOne(UserClass, name='Grant')

    # increment the age of the user by 1
    user['age'] += 1
    # Thats it! the database has now been updated with the new age

Drop a table
------------

::

    from dictabase import Drop
    Drop(UserClass, confirm=True)
    # the table has been removed from the database

Delete a specific row in a table
--------------------------------

::

    from dictabase import Delete
    # find an object that was previously put in the database
    user = FindOne(UserClass, name='Grant')
    Delete(user)
    # the user has been removed from the database

Advanced Usage
--------------
You can only store simple types like int, str, datetime in the database.
To store more complicated objects, like list, dict, or any arbitrary type, override the DumpKey() and LoadKey() method.
Using these methods, you can convert complex types into these more simple types.

::

    from dictabase import (
        BaseTable,
        FindOne,
        New,
        RegisterDBURI,
        Drop,
    )
    import json

    RegisterDBURI()


    class Page(BaseTable):
        pass


    Drop(Page, confirm=True) # clear the table for this test


    class Book(BaseTable):
        def DumpKey(self, key, value):
            # this is called when putting info into the database
            if key == 'pages':
                ret = [page['id'] for page in value] # only store the page id as a json'd list of ints
                return json.dumps(ret)
            else:
                return value

        def LoadKey(self, key, value):
            # this is called when extracting info from the database
            if key == 'pages':
                ret = json.loads(value)
                ret = [FindOne(Page, id=ID) for ID in ret]
            else:
                return value


    Drop(Book, confirm=True) # clear the table for this test

    # Create a new book
    book = New(Book)
    book['pages'] = [] # this will hold our pages

    print('book=', book)
    >> book= <Book: id=1(type=int), pages=[](type=list)>

    # Fill the book with pages
    for i in range(10):
        page = New(
            Page,
            words='These are words {}'.format(i),
            parentBookID=book['id']
        )
        book['pages'].append(page)

    print('book=', book)
    >> book= <Book: id=1(type=int), pages=[<Page: words=These are words 0(type=str), parentBookID=1(type=int), id=1(type=int)>, <Page: words=These are words 1(type=str), parentBookID=1(type=int), id=2(type=int)>, <Page: words=These are words 2(type=str), parentBookID=1(type=int), id=3(type=int)>, <Page: words=These are words 3(type=str), parentBookID=1(type=int), id=4(type=int)>, <Page: words=These are words 4(type=str), parentBookID=1(type=int), id=5(type=int)>, <Page: words=These are words 5(type=str), parentBookID=1(type=int), id=6(type=int)>, <Page: words=These are words 6(type=str), parentBookID=1(type=int), id=7(type=int)>, <Page: words=These are words 7(type=str), parentBookID=1(type=int), id=8(type=int)>, <Page: words=These are words 8(type=str), parentBookID=1(type=int), id=9(type=int)>, <Page: words=These are words 9(type=str), parentBookID=1(type=int), id=10(type=int)>](type=list)>

    # Look up the book/pages in the database
    foundBook = FindOne(Book)

    print('foundBook=', foundBook)
    >> foundBook= <Book: id=1(type=int), pages=[<Page: words=These are words 0(type=str), parentBookID=1(type=int), id=1(type=int)>, <Page: words=These are words 1(type=str), parentBookID=1(type=int), id=2(type=int)>, <Page: words=These are words 2(type=str), parentBookID=1(type=int), id=3(type=int)>, <Page: words=These are words 3(type=str), parentBookID=1(type=int), id=4(type=int)>, <Page: words=These are words 4(type=str), parentBookID=1(type=int), id=5(type=int)>, <Page: words=These are words 5(type=str), parentBookID=1(type=int), id=6(type=int)>, <Page: words=These are words 6(type=str), parentBookID=1(type=int), id=7(type=int)>, <Page: words=These are words 7(type=str), parentBookID=1(type=int), id=8(type=int)>, <Page: words=These are words 8(type=str), parentBookID=1(type=int), id=9(type=int)>, <Page: words=These are words 9(type=str), parentBookID=1(type=int), id=10(type=int)>](type=list)>

    for page in foundBook['pages']:
        print('page=', page)
    >> page= <Page: words=These are words 0(type=str), parentBookID=1(type=int), id=1(type=int)>
    >> page= <Page: words=These are words 1(type=str), parentBookID=1(type=int), id=2(type=int)>
    >> page= <Page: words=These are words 2(type=str), parentBookID=1(type=int), id=3(type=int)>
    >> page= <Page: words=These are words 3(type=str), parentBookID=1(type=int), id=4(type=int)>
    >> page= <Page: words=These are words 4(type=str), parentBookID=1(type=int), id=5(type=int)>
    >> page= <Page: words=These are words 5(type=str), parentBookID=1(type=int), id=6(type=int)>
    >> page= <Page: words=These are words 6(type=str), parentBookID=1(type=int), id=7(type=int)>
    >> page= <Page: words=These are words 7(type=str), parentBookID=1(type=int), id=8(type=int)>
    >> page= <Page: words=These are words 8(type=str), parentBookID=1(type=int), id=9(type=int)>
    >> page= <Page: words=These are words 9(type=str), parentBookID=1(type=int), id=10(type=int)>
