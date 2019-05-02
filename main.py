from dictabase import (
    _BaseDictabaseTable,
    Drop,
    FindAll,
    FindOne,
    Delete,
)
import random


class Person(_BaseDictabaseTable):
    # Each subclass of _BaseDictabaseTable produces another table in the db
    pass


class Animal(_BaseDictabaseTable):
    pass


# For testing, delete all tables first
# Comment these out to make data persistant
Drop(Person)
Drop(Animal)

# Create tables with random data
for i in range(10):
    # Instantiating a new Person object adds a new row in the db
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

# FindOne() returns an object found in the database
person5 = FindOne(Person, name='Name5')
print('Age of Person5=', person5['age'])

# Remove any animlas with age >= 5
for animal in FindAll(Animal):
    if animal['age'] >= 5:
        print('Removing animal=', animal)
        Delete(animal)

print('Remaining Animals=', FindAll(Animal))

# Notice that now your project space contains a MyDatabase.db which is a sqlite database.
