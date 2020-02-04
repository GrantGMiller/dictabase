from dictabase import (
    BaseDictabaseTable,
    Drop,
    FindAll,
    FindOne,
    Delete,
)
import random


class Person(BaseDictabaseTable):
    # Each subclass of BaseDictabaseTable produces another table in the db
    pass


class Animal(BaseDictabaseTable):
    pass


# For testing, delete all tables first
# Comment these out to make data persistant
Drop(Person)
Drop(Animal)

# Create tables with random data
for i in range(10):
    # Instantiating a new Person obj adds a new row in the db
    newPerson = Person(
        name='Name{}'.format(i),
        age=random.randint(1, 100),
    )
    print('newPerson=', newPerson)

    newAnimal = Animal(
        kind=random.choice(['Cat', 'Dog']),
        name='Fluffy{}'.format(i),
        age=random.randint(1, 10),
    )
    print('newAnimal=', newAnimal)

# FindAll() returns all items from the database that match
# you can also use keywords like '_limit', '_reverse', '_orderBy'
print('Number of animals of age 5: {}'.format(
    len(FindAll(Animal, age=5)))
)

# FindOne() returns an obj found in the database
person5 = FindOne(Person, name='Name5')
print('Age of Person5=', person5['age'])

# Remove any animlas with age >= 5
for animal in FindAll(Animal):
    if animal['age'] >= 5:
        print('Removing animal=', animal)
        Delete(animal)

print('Remaining Animals=', FindAll(Animal))


# Test Relational Mapping

class Book(BaseDictabaseTable):
    pass


class Page(BaseDictabaseTable):
    pass


Drop(Book)
Drop(Page)

book = Book(title='Title')
page1 = Page(words='Words1')
page2 = Page(words='Words2')

print('77 book=', book)
print('78 page1=', page1)
print('79 page2=', page2)

book['pages'] = [page1, page2]
page1['book'] = book
page2['book'] = book

print('book["pages"]=', book['pages'])
print('page1["book"]=', page1['book'])
print('page2["book"]=', page2['book'])

# Notice that now your project space contains a MyDatabase.db which is a sqlite database.
