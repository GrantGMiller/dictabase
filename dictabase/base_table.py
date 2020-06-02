global db
db = None


def RegisterDbWorker(newDB):
    global db
    db = newDB


global DEBUG


def SetDebug(newState):
    global print
    global DEBUG
    DEBUG = newState
    if newState is False:
        print = lambda *a, **k: None


class BaseTable(dict):
    def LoadKey(self, key, dbValue):
        # moving data from database to the BaseTable object
        return dbValue

        # use below template
        # return {
        #     'pages': lambda v: json.loads(v),
        # }.get(key, lambda v: v)(dbValue)

    def DumpKey(self, key, value):
        # moving data from the BaseTable object to the database
        return value

        # use below as template
        # return {
        #     'pages': lambda v: json.dumps(v, indent=2, sort_keys=True),
        # }.get(key, lambda v: v)(objValue)

    def __del__(self):
        print(f'{self}.__del__()')
        db.Upsert(self)

    def __str__(self):
        '''

        :return: string like '<BaseDictabaseTable: email=me@website.com, name=John>'
        '''
        itemsList = []
        for k, v, in self.items():
            if k.startswith('_'):
                if DEBUG is False:
                    continue  # dont print these

            if isinstance(v, str) and len(v) > 25:
                v = v[:25] + '...'
            itemsList.append(('{}={}(type={})'.format(k, v, type(v).__name__)))

        if DEBUG:
            itemsList.append(('{}={}'.format('pyid', id(self))))

        return '<{}: {}>'.format(
            type(self).__name__,
            ', '.join(itemsList)
        )

    def __repr__(self):
        return str(self)
