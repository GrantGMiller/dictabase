Help on module dictabase:

NAME
    dictabase

CLASSES
    builtins.dict(builtins.object)
        BaseDictabaseTable
    
    class BaseDictabaseTable(builtins.dict)
     |  This class saves any changes to a database.
     |  You should subclass this class to create a new table.
     |  
     |  For example:
     |      class UserClass(BaseDictabaseTable):
     |          pass
     |  
     |      user = UserClass(email='me@website.com', name='John')
     |  
     |      # Then later in your code you can call
     |  
     |      result = FindOne(UserClass, email='me@website.com')
     |      print('result=', result)
     |      >> result= UserClass(email='me@website.com', name='John')
     |  
     |  Method resolution order:
     |      BaseDictabaseTable
     |      builtins.dict
     |      builtins.object
     |  
     |  Methods defined here:
     |  
     |  AfterInsert(self, *args, **kwargs)
     |      Override this method to do something after object is inserted in database
     |      
     |      Example:
     |      
     |      class Post(BaseDictabaseTable):
     |      
     |          def AfterInsert(self):
     |              self['insertionTimestamp'] = datetime.datetime.now()
     |  
     |  CustomGetKey(self, key, value)
     |      This module relies on the types supported by the dataset package.
     |      If you have a custom type, you can use override this method to change the behavior of getting a value from the db
     |      
     |      Example:
     |      
     |      class Post(BaseDictabaseTable):
     |      
     |          def CustomGetKey(self, key, value):
     |              if key == 'content':
     |                  return key, Markup(value) # cast the value as a flask.Markup object
     |              else:
     |                  return key, value # return the value normally
     |  
     |  CustomSetKey(self, key, value)
     |      This module relies on the types supported by the dataset package.
     |      If you have a custom type, you can use this method to convert it to a supported type before writing it to the db
     |      
     |      :param key:
     |      :param value:
     |      :return: tuple of (newKey, newValue)
     |      
     |      Example:
     |      
     |      class MyCustomClass(BaseDictabaseTable):
     |          def CustomSetKey(self, key, value):
     |              if key == 'CustomKeyNotSupportedByDataSet':
     |                  value = dict(value)
     |                  return key, value
     |              else:
     |                  return key, value
     |  
     |  __getitem__(self, key)
     |      x.__getitem__(y) <==> x[y]
     |  
     |  __init__(self, *args, **kwargs)
     |      Accepts same args/kwargs as normal python dict
     |      
     |      :param args:
     |      :param kwargs:
     |  
     |  __repr__(self)
     |      Return repr(self).
     |  
     |  __setitem__(self, key, value)
     |      Any time a value is set to this object, the change will be updated in the database
     |      :param key:
     |      :param value:
     |      :return:
     |  
     |  __str__(self)
     |      :return: string like '<BaseDictabaseTable: email=me@website.com, name=John>'
     |  
     |  get(self, *a, **k)
     |      Works the same as the built in python dict.get
     |      :param a:
     |      :param k:
     |      :return:
     |  
     |  ----------------------------------------------------------------------
     |  Data descriptors defined here:
     |  
     |  __dict__
     |      dictionary for instance variables (if defined)
     |  
     |  __weakref__
     |      list of weak references to the object (if defined)
     |  
     |  ----------------------------------------------------------------------
     |  Data and other attributes defined here:
     |  
     |  uniqueKeys = ['id']
     |  
     |  ----------------------------------------------------------------------
     |  Methods inherited from builtins.dict:
     |  
     |  __contains__(self, key, /)
     |      True if D has a key k, else False.
     |  
     |  __delitem__(self, key, /)
     |      Delete self[key].
     |  
     |  __eq__(self, value, /)
     |      Return self==value.
     |  
     |  __ge__(self, value, /)
     |      Return self>=value.
     |  
     |  __getattribute__(self, name, /)
     |      Return getattr(self, name).
     |  
     |  __gt__(self, value, /)
     |      Return self>value.
     |  
     |  __iter__(self, /)
     |      Implement iter(self).
     |  
     |  __le__(self, value, /)
     |      Return self<=value.
     |  
     |  __len__(self, /)
     |      Return len(self).
     |  
     |  __lt__(self, value, /)
     |      Return self<value.
     |  
     |  __ne__(self, value, /)
     |      Return self!=value.
     |  
     |  __new__(*args, **kwargs) from builtins.type
     |      Create and return a new object.  See help(type) for accurate signature.
     |  
     |  __sizeof__(...)
     |      D.__sizeof__() -> size of D in memory, in bytes
     |  
     |  clear(...)
     |      D.clear() -> None.  Remove all items from D.
     |  
     |  copy(...)
     |      D.copy() -> a shallow copy of D
     |  
     |  fromkeys(iterable, value=None, /) from builtins.type
     |      Returns a new dict with keys from iterable and values equal to value.
     |  
     |  items(...)
     |      D.items() -> a set-like object providing a view on D's items
     |  
     |  keys(...)
     |      D.keys() -> a set-like object providing a view on D's keys
     |  
     |  pop(...)
     |      D.pop(k[,d]) -> v, remove specified key and return the corresponding value.
     |      If key is not found, d is returned if given, otherwise KeyError is raised
     |  
     |  popitem(...)
     |      D.popitem() -> (k, v), remove and return some (key, value) pair as a
     |      2-tuple; but raise KeyError if D is empty.
     |  
     |  setdefault(...)
     |      D.setdefault(k[,d]) -> D.get(k,d), also set D[k]=d if k not in D
     |  
     |  update(...)
     |      D.update([E, ]**F) -> None.  Update D from dict/iterable E and F.
     |      If E is present and has a .keys() method, then does:  for k in E: D[k] = E[k]
     |      If E is present and lacks a .keys() method, then does:  for k, v in E: D[k] = v
     |      In either case, this is followed by: for k in F:  D[k] = F[k]
     |  
     |  values(...)
     |      D.values() -> an object providing a view on D's values
     |  
     |  ----------------------------------------------------------------------
     |  Data and other attributes inherited from builtins.dict:
     |  
     |  __hash__ = None

FUNCTIONS
    Delete(obj)
        Delete a row from the database
        
        :param obj: subclass of dict
        :return: None
    
    Drop(objType)
        Delete an entire table from the database
        
        :param objType:
        :return: None
    
    FindAll(objType, **k)
        Find all object in database that match the **k
        
        Also pass special kwargs to return objects in a certain order/limit
        
        FindAll(MyClass, _reverse=True) > returns all objects in reverse order
        
        FindAll(MyClass, _orderBy='Name') > returns all objects sorted by the "Name" column
        
        FindAll(MyClass, _limit=5) > return first 5 matching objects
        
        :param objType: type
        :param k: an empty dict like {} will return all items from table
        :return: a generator that will iterate thru all the results found, may have length 0
    
    FindOne(objType, **k)
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
    
    SetDB_URI(dburi)
        Set the URI for the database.
        Supports any URI supported by SQLAlchemy. Defaults to sqllite
        :param dburi: str like 'sqlite:///MyDatabase.db'
        :return:

FILE
    c:\users\gmiller\pycharmprojects\dictabase\dictabase.py


