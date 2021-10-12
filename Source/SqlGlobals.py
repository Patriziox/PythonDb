from enum import Enum, auto, unique


SHA256_SIZE = 64

DB_EXT = '.db'

TABLE_PREFIX = 'TAB_'
TABLE_DEF_EXT = '.def'
TABLE_DAT_EXT = '.dat'
TABLE_HEAD_EXT = '.head'
TABLE_UNIQUE_EXT = '.unique'


MAIN_FOLDER = 'DBMain'

DB_FOLDER = f'{MAIN_FOLDER}\\Database'

SYS_FOLDER = f'{MAIN_FOLDER}\\Sys'
TABLES_FOLDER = 'Tables'
USER_DATA = f'{SYS_FOLDER}\\users.dat'

@unique
class enum_DataType(Enum):
    eNone = 0
    eInt = 1
    eVarchar = 2
    eBool = 3
    eDate = 4
    eTime = 5

class enum_TcpToken(Enum):
    
    eReplyNone = 0
    eReplySuccessful = auto()
    eReplyFailure = auto()
    eReplyUnknownCommand = auto()
    eReplySyntaxError = auto()
    eReplyNotLogged = auto()
    eReplyExit = auto()
    eReplyBadPassword = auto()
    eReplyUserNameAlredyUsed = auto()
    eReplyDBNameAlredyUsed = auto()
    eReplyTuple = auto()
    eReplyOverWrite = auto()
    eReqQuery = auto()
    
