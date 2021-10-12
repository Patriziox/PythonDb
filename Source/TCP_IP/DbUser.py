import pickle

import SqlGlobals as Glob
from SqlGeneric import SqlGeneric as Gen

from FileSystem import FileSystem
from enum import Enum, unique
from SqlError import *



@unique
class enum_UserFlag(Enum):
   
   eEditUser    = 0
   eCreateDB    = 1
   eDropDB      = 2

class DbUserModel :
    
    THIS_QNT_FLAG = 32
    
    def __init__(self) :
        self.m_sUserName    = ''
        self.m_sPwdSha256   = None
        self.m_sPwdFoot     = None
        self.m_vbUserFlag   = [False] * DbUserModel.THIS_QNT_FLAG

    def GetUserName(self) -> str:
        return self.m_sUserName
    
    def SetUserName(self, sUserName : str) :
        self.m_sUserName = sUserName
    
    def GetPassword(self) -> tuple:
        return self.m_sPwdSha256, self.m_sPwdFoot

    def SetPassword(self, sPassword : str, sFoot : str = None) :
        self.m_sPwdSha256, self.m_sPwdFoot = Gen.SetPassword(sPassword, sFoot)

    def GetUserFlag(self, iIndex : int) -> bool :
        return self.m_vbUserFlag[iIndex]
    
    def SetUserFlag(self, iIndex : int, bState : bool) :
        self.m_vbUserFlag[iIndex] = bState

    def Serialize(self, fTarget = None) -> bytes or bool :

        try :
                        
            if fTarget :
                pickle.dump(self, fTarget)

                return True

            return pickle.dumps(self)
        
        except :

            return False

    def Deserialize(self, oSource) -> int :

        try :

            if isinstance(oSource, bytes) :
                
                oUserItem = pickle.loads(oSource) 
                iQntByte = len(oSource)
            
            else :

                iQntByte = oSource.tell()
                oUserItem = pickle.load(oSource)

                iQntByte = oSource.tell() - iQntByte

            self.m_sUserName    = oUserItem.m_sUserName
            self.m_sPwdSha256   = oUserItem.m_sPwdSha256
            self.m_sPwdFoot     = oUserItem.m_sPwdFoot
            self.m_vbUserFlag   = oUserItem.m_vbUserFlag

            return iQntByte

        except :

            return 0

class DbUser:
   
    THIS_DEFAULT_LOGIN_PARS = 'admin'
   
    def __init__(self) :
        self.m_oDbUserItem  = DbUserModel()
        self.m_iOffset      = None
    
    @staticmethod
    def Initialize() -> enum_Error:
       
        if FileSystem.FileNotEmpty(Glob.USER_DATA) :
            return enum_Error.eNone

        oUser = DbUser()
        oUser.SetUserName(DbUser.THIS_DEFAULT_LOGIN_PARS)
        oUser.SetPassword(DbUser.THIS_DEFAULT_LOGIN_PARS)
        
        for eFlag in enum_UserFlag :
            oUser.SetUserFlag(eFlag.value, True)

        return oUser.Save()
 
    def GetUserName(self) -> str:
        return self.m_oDbUserItem.GetUserName()
    
    def SetUserName(self, sUserName : str):
        self.m_oDbUserItem.SetUserName(sUserName)
    
    def GetPassword(self) -> str:
        sPwd, sFoor = self.m_oDbUserItem.GetPassword()
        return sPwd

    def SetPassword(self, sPassword : str):
        self.m_oDbUserItem.SetPassword(sPassword)

    def GetUserFlag(self, iIndex : int) -> bool:
        return self.m_oDbUserItem.GetUserFlag(iIndex)
    
    def SetUserFlag(self, iIndex : int, bState : bool):
       self.m_oDbUserItem.SetUserFlag(iIndex, bState)

    def Load(self, sUserName : str) -> enum_Error:
            
         with FileSystem() as oFileUser :
            try :
                
                eError = oFileUser.Open(Glob.USER_DATA, 'rb')

                if eError :
                    return eError

                oUserItem = DbUserModel()

                iSize = FileSystem.Size(Glob.USER_DATA)

                iOffset = 0

                while iOffset < iSize :

                    iQntByte = oUserItem.Deserialize(oFileUser.Raw())
                    
                    if oUserItem.GetUserName() == sUserName :
                        self.m_oDbUserItem = oUserItem
                        self.m_iOffset = iOffset

                        return enum_Error.eNone

                    iOffset += iQntByte

                return enum_Error.eNotFound

            except :
        
                return enum_Error.eFailure

    def Save(self) -> enum_Error :

        with FileSystem() as oFileUser :
            try :
                
                if self.m_iOffset is None:
                    
                    eError = oFileUser.Open(Glob.USER_DATA, 'ab')

                    if eError :
                        return eError
                
                else :

                    eError = oFileUser.Open(Glob.USER_DATA, 'r+b')

                    if eError :
                        return eError

                    oFileUser.Seek(self.m_iOffset, FileSystem.SEEK_SET)

                oResult = self.m_oDbUserItem.Serialize(oFileUser.Raw())

                return enum_Error.eNone if oResult else enum_Error.eFailure
               
            except :
        
                return enum_Error.eFailure

    def Check(self, sPassword : str) -> bool :

        myPwd, sFoot = self.m_oDbUserItem.GetPassword()

        oDbUserItem = DbUserModel()
        oDbUserItem.SetPassword(sPassword, sFoot)
       
        extPwd, sFoot = oDbUserItem.GetPassword()

        return (myPwd == extPwd)
    