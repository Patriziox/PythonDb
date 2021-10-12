import pickle
from enum import Enum, unique

@unique
class enum_Error(Enum):
   
    eNone = 0,
    eSyntax = 1,
    eBadValue = 2,
    eConstraitVioletion = 3,
    eUnknownItem = 4,
    eNotPossible = 5,
    eOffline = 6,
    eNotFound = 7,
    eIOError = 8,
    eEof = 9,
    eFailure = 10,
    eBadPwd = 11,
    eUnspecified = 99,

    # select Error
    eClauseMissing = 100,
    eTableMissing = 101,
    eTableUndefined = 102,
    eColumnUndefined = 103,
    eAmbiguousReference = 104,
    eClauseVioletion = 105,

#end enum

class SqlError:
   	  
    def __init__(self, eError = enum_Error.eNone, sNote = ''):
        self.m_eError = eError
        self.m_sNote = sNote
  
    def __eq__(self, it):

        if(it == True):
            return (self.m_eError is not enum_Error.eNone)

        if(it == False):
            return (self.m_eError is enum_Error.eNone)

        if isinstance(it, enum_Error) :
            return self.m_eError is it

        if isinstance(it, SqlError) :
            return self.m_eError is it.m_eError

        return True

    def __bool__(self):
	    return (self.m_eError is not enum_Error.eNone)

    def GetError(self):
        return self.m_eError
        
    def GetNote(self, sNote = None):

        if(sNote != None):
            self.m_sNote = sNote

        return self.m_sNote

    def Serialize(self) :
        return pickle.dumps(tuple((self.m_eError, self.m_sNote)))
  
#end class