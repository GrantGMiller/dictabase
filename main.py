from dictabase import (
    _BaseDictabaseTable,
    Drop,
    FindAll,
    FindOne,
    Delete,
)
import random


class Person(_BaseDictabaseTable):
    pass


class Animal(_BaseDictabaseTable):
    pass


# For testing, delete all tables first
Drop(Person)
Drop(Animal)

# Create tables with random data
for i in range(10):
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

print('Number of animals of age 5: {}'.format(
    len(FindAll(Animal, age=5)))
)

person5 = FindOne(Person, name='Name5')
print('Age of Person5=', person5['age'])

# Remove any animlas with age >= 5
for animal in FindAll(Animal):
    if animal['age'] >= 5:
        print('Removing animal=', animal)
        Delete(animal)

print('Remaining Animals=', FindAll(Animal))


