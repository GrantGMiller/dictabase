A database interface that acts like a python dictionary

# Example
[Click here to see a complete example main.py](https://github.com/GrantGMiller/dictabase/blob/master/main.py)

## Create a new table
```
# Create a table representing users
from dictabase import BaseDictabaseTable
class UserClass(BaseDictabaseTable):
    pass
    
newUser = UserClass(name='Grant', age=31)
newUser = UserClass(name='Bob', age=99)
# there is now a sqlite database containing the 2 users. Yup, thats it!
```
## Look up items in the database
```
from dictabase import FindOne, FindAll

allUsers = FindAll(UserClass)
# allUsers is an iterable of all the UserClass objects that exists in the database
print('allUsers=', list(allUsers))
>> allUsers= [<UserClass: name='Grant'>, <UserClass: name='Bob'>]

user = FindOne(UserClass, name='Grant')
print('user=', user)
>>user= <UserClass: name='Grant'>

user = FindOne(UserClass, name='NotARealName')
print('user=', user)
>>user= None
```

## Read/Write to the database
```
# find an object that was previously put in the database
user = FindOne(UserClass, name='Grant')

# increment the age of the user by 1
user['age'] = user['age'] + 1
# Thats it! the database has now been updated with the new age
```

## Drop a table
```
from dictabase import Drop
Drop(UserClass)
# the table has been removed from the database

```

## Delete a specific row in a table
```
from dictabase import Delete
# find an object that was previously put in the database
user = FindOne(UserClass, name='Grant')
Delete(user)
# the user has been removed from the database
```
