from SQL.SqlQuery import *
from FileSystem import *
from SQL.SqlTables import SqlTables

class SqlDataBaseModel:

    def __init__(self, sDbName : str = None) :

        self.m_sName        = sDbName
        self.m_sPwdSha256   = None
        self.m_sPwdFoot     = None

    def GetName(self) -> str:
        return self.m_sName

    def SetPassword(self, sPassword : str):
        self.m_sPwdSha256, self.m_sPwdFoot = Gen.SetPassword(sPassword)

    def Serialize(self, fTarget) -> bool :

        try :
                        
            pickle.dump(self, fTarget)

            return True
        
        except :

            return False

    def Deserialize(self, fSource) -> bool :

        try :

            oDbItem = pickle.load(fSource)

            self.m_sName        = oDbItem.m_sName
            self.m_sPwdSha256   = oDbItem.m_sPwdSha256
            self.m_sPwdFoot     = oDbItem.m_sPwdFoot

            return True

        except :

            return False
    
    def CheckForPwd(self, sPwd : str) -> bool :
        
        sPwdSha256, sFoot = Gen.SetPassword(sPwd, self.m_sPwdFoot)

        return (sPwdSha256 == self.m_sPwdSha256)

class SqlDataBase:

    def __init__(self) :
        self.Kill()
    
    def Kill(self) :        
        
        self.m_oDataBase = None
        self.m_sRoot = None
        self.m_oTables = SqlTables()

    def GetRoot(self) -> str:
        return self.m_sRoot

    def GetName(self) -> str:
        return self.m_oDataBase.GetName()
   
    def SetPassword(self, sPassword : str):
        self.m_oDataBase.SetPassword(sPassword)

    def Create(self, sDbName : str, sRoot : str, sPwd : str) -> SqlError:
        
        self.m_oDataBase = SqlDataBaseModel(sDbName)

        self.m_sRoot = sRoot
        self.m_oDataBase.SetPassword(sPwd)

        sFolder = f'{sRoot}\\{sDbName}\\{Glob.TABLES_FOLDER}'
        
        FileSystem.CreateFolder(sFolder)

        sFullPath = f'{sRoot}\\{sDbName}\\{sDbName}{Glob.DB_EXT}'

        if FileSystem.FileNotEmpty(sFullPath) :

            return self.Load(sFullPath, sPwd)


        eError = self.Save()

        if eError :

            self.Kill()

            FileSystem.RemoveFolder(sFolder)
        
        return eError

    def Tables(self) -> SqlTables:
        return self.m_oTables

    def Query(self, sQuery : str) -> bool:
        return SqlQuery(self).Parse(sQuery)
    
    def GetTables(self) -> SqlTables :
        return self.m_oTables
 
    def Load(self, sFullPath : str, sPwd : str) -> SqlError:
        
        with FileSystem() as oDbFile :
        
            eError = oDbFile.Open(sFullPath, 'rb')

            if eError :
                return eError

            oDataBase = SqlDataBaseModel()

            if not oDataBase.Deserialize(oDbFile.Raw()) :
                return SqlError(enum_Error.eFailure)
        
            if not oDataBase.CheckForPwd(sPwd) :
                return SqlError(enum_Error.eBadPwd)
            
            self.m_oDataBase = oDataBase

            self.m_sRoot = oDbFile.GetPath()
        
            self.m_oTables.Initialize(self.m_sRoot)

            return SqlError()

    def Save(self) -> enum_Error:

        sFullPath = f'{self.m_sRoot}\\{self.m_oDataBase.GetName()}\\{self.m_oDataBase.GetName()}{Glob.DB_EXT}'
                
        with FileSystem() as oDbFile :

            eError = oDbFile.Open(sFullPath, 'wb')

            if eError :
                return eError

            bResult = self.m_oDataBase.Serialize(oDbFile.Raw())

            return SqlError(enum_Error.eNone if bResult else enum_Error.eFailure)
