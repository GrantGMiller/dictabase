import time


def LoadKeys(obj):
    for k, v in obj.copy().items():
        obj[k] = obj.LoadKey(k, v)
    return obj


def DumpKeys(obj):
    baseTableObj = obj
    dictObj = baseTableObj.copy()
    for k, v in baseTableObj.items():
        dictObj[k] = baseTableObj.DumpKey(k, v)
    return dictObj


def IsEmpty(d):
    isEmpty = True
    for theType in d:
        if len(d[theType]) > 0:
            isEmpty = False
            break

    return isEmpty


class ExponentialDelay:
    def __init__(self, seed=0.1):
        self._delay = seed

    def Delay(self, state):
        if state:
            self._delay *= 1.1
            print('time.sleep(', self._delay)
            time.sleep(self._delay)
        return state


def IsSubset(subDict, superDict):
    return all(item in superDict.items() for item in subDict.items())
