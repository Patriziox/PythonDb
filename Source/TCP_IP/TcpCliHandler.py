from SQL.SqlDataBase import SqlDataBase
import socket
from enum import Enum, unique

from SqlGeneric import SqlGeneric as Gen
from SqlGlobals import enum_TcpToken

from TCP_IP.TcpCommands import *
from TCP_IP.DbUser import *
from TCP_IP.TcpSocket import *
from FileSystem import *

@unique
class enum_TcpCommands(Enum):
   
   eExit = 0
   eLogin = 1
   eChangePwd = 2
   eAddUser = 3

class TcpCliHandler:

    THIS_DEFAULT_LOGIN_PAR = 'admin'
    THIS_TOT_BYTE = 4096
  
    st_tCommands = (
        ('EXIT', '_ExitHnd'),
        ('LOGIN', '_LoginHnd'),
        ('PWD', '_PasswordHnd'),
        ('CREATEUSER', '_CreateUserHnd'),
        ('CREATEDB', '_CreateDbHnd'),
        ('OPENDB', '_OpenDbHnd'),
        ('CLOSEDB', '_CloseDbHnd'),
    )
      
    def __init__(self, oSocket : socket) -> None:
        self.m_oTcpSocket   = TcpSocket(oSocket)
        self.m_oUser        = None
        self.m_oDataBase    = None

    def _ExitHnd(self) -> enum_TcpToken and str:
        return enum_TcpToken.eReplyExit, ''

    def _LoginHnd(self, sQuery) -> enum_TcpToken and str:
       
        vsCuts = Gen.Cut(sQuery, ' ')

        if len(vsCuts) != 3 :
            return enum_TcpToken.eReplySyntaxError, ''

        sUserName   = vsCuts[1].replace('"', '')
        sPwd        = vsCuts[2].replace('"', '')
               
        oUser = DbUser()

        eReply = enum_TcpToken.eReplyNotLogged

        if oUser.Load(sUserName) :
            if oUser.Check(sPwd) :
                
                self.m_oUser = oUser
                eReply = enum_TcpToken.eReplySuccessful

            else :

                eReply = enum_TcpToken.eReplyBadPassword


        return eReply, ''

    def _PasswordHnd(self, sQuery : str) -> enum_TcpToken and str:

        vsCuts = Gen.Cut(sQuery, ' ')

        if len(vsCuts) != 3 :
            return enum_TcpToken.eReplySyntaxError, ''
      

        sOldPwd = vsCuts[1].replace('"', '')
        sNewPwd = vsCuts[2].replace('"', '')

        if self.m_oUser.Check(sOldPwd) == False:
            return enum_TcpToken.eReplyBadPassword, ''
        
        self.m_oUser.SetPassword(sNewPwd)

        eError = self.m_oUser.Save()

        return enum_TcpToken.eReplySuccessful if (eError == enum_Error.eNone) else enum_TcpToken.eReplyFailure, ''

    def _CreateUserHnd(self, sQuery : str) -> enum_TcpToken and str:
        
        vsCuts = Gen.Cut(sQuery, ' ')

        iQntFields = len(vsCuts)

        if iQntFields < 3 :
            return enum_TcpToken.eReplySyntaxError, ''

        sUserName   = vsCuts[1].replace('"', '')
        sPwd        = vsCuts[2].replace('"', '')
               
        vsCuts = [] if iQntFields == 3 else Gen.Cut(vsCuts[3], ',')

        oUser = DbUser()

        if (oUser.Load(sUserName) == enum_Error.eNone):
            eReply = enum_TcpToken.eReplyUserNameAlredyUsed
        
        else :
            
            oUser = DbUser()
            
            oUser.SetUserName(sUserName)
            oUser.SetPassword(sPwd)

            eReply = enum_TcpToken.eReplySuccessful

            for iIndex, sFlag in enumerate(vsCuts) :
                
                if sFlag not in ('0', '1', '') :
                    eReply = enum_TcpToken.eReplySyntaxError
                    break

                if sFlag != '' :    
                    oUser.SetUserFlag(iIndex, (sFlag == '1'))

            if eReply == enum_TcpToken.eReplySuccessful : 
                if oUser.Save() != enum_Error.eNone:
                    eReply = enum_TcpToken.eReplyFailure

        return eReply, ''

    def _CreateDbHnd(self, sQuery : str)-> enum_TcpToken and str:
        
        vsCuts = Gen.Cut(sQuery, ' ')

        if len(vsCuts) != 3 :
            return enum_TcpToken.eReplySyntaxError, ''
        
        sDBName   = vsCuts[1].replace('"', '')
        sPwd      = vsCuts[2].replace('"', '')

        sDBFolder = f'{Glob.DB_FOLDER}\\{sDBName}'

        FileSystem.CreateFolder(sDBFolder)
        
        oDataBase = SqlDataBase()
        
        eError = oDataBase.Create(sDBName, Glob.DB_FOLDER, sPwd) 

        if not eError :

            self.m_oDataBase = oDataBase
            return enum_TcpToken.eReplySuccessful, ''

        if eError == enum_Error.eNotPossible :
            return enum_TcpToken.eReplyDBNameAlredyUsed, ''

        FileSystem.RemoveFolder(sDBFolder)

        return enum_TcpToken.eReplyFailure, ''

    def _OpenDbHnd(self, sQuery : str)-> enum_TcpToken and str:
        
        vsCuts = Gen.Cut(sQuery, ' ')

        if len(vsCuts) != 3 :
            return enum_TcpToken.eReplySyntaxError, ''
        
        sDBName   = vsCuts[1].replace('"', '')
        sPwd      = vsCuts[2].replace('"', '')

        oDataBase = SqlDataBase()
        
        sFullPath = f'{Glob.DB_FOLDER}\\{sDBName}\\{sDBName}{Glob.DB_EXT}'

        eError = oDataBase.Load(sFullPath, sPwd) 

        if not eError :

            self.m_oDataBase = oDataBase
            return enum_TcpToken.eReplySuccessful, ''
        
        return enum_TcpToken.eReplyFailure, ''

    def _CloseDbHnd(self, sQuery : str) -> enum_TcpToken and str :
        self.m_oDataBase = None

        return enum_TcpToken.eReplySuccessful, ''

    def _QueryHnd(self, sQuery : str) -> enum_TcpToken and (bytes or None) :
        
        return self.m_oDataBase.Query(sQuery)

    def Run(self):
            
        while(True):

            oTcpFrame = self.m_oTcpSocket.Recv(TcpCliHandler.THIS_TOT_BYTE)

            if not oTcpFrame :
                continue
                        
            sQuery = Gen.Normalize(oTcpFrame.GetData())

            iIndex, oHandler, sHead, sFoot = Gen.GetHandler(self, sQuery, TcpCliHandler.st_tCommands)

            if iIndex == enum_TcpCommands.eExit.value :
                eReply, sData = self._ExitHnd()
                self.m_oTcpSocket.Send(eReply, sData)
                return eReply

            if (self.m_oUser == None) and (iIndex != enum_TcpCommands.eLogin.value) :
                self.m_oTcpSocket.Send(enum_TcpToken.eReplyNotLogged)
                continue

            if oHandler == None :
                
                if self.m_oDataBase != None :
                    
                    eReply, sData = self._QueryHnd(sQuery)
                   
                    self.m_oTcpSocket.Send(eReply, sData)
                    continue

                self.m_oTcpSocket.Send(enum_TcpToken.eReplyUnknownCommand)
                continue
                      
            eReply, sData = oHandler(sQuery)

            self.m_oTcpSocket.Send(eReply, len(sData), sData)

        return eReply