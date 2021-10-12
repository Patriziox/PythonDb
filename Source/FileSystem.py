import os
import shutil
from os import SEEK_CUR, path
from SqlError import *
from SqlGeneric import SqlGeneric as Gen


class FileSystem:

    SEEK_CUR = os.SEEK_CUR
    SEEK_SET = os.SEEK_SET
    SEEK_END = os.SEEK_END

    def __init__(self) -> None:
        self._Kill()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_tb) :
        if self.m_fTarget :
            self.m_fTarget.close()
        
        return True
    
    @staticmethod
    def FileExist(sTarget : str) -> bool :
        return path.isfile(sTarget)

    @staticmethod
    def FileNotEmpty(sTarget : str) -> bool :
        if not path.isfile(sTarget) :
            return False
        
        return (os.stat(sTarget).st_size > 0)

    @staticmethod
    def Size(sTarget : str) -> int :
        return os.stat(sTarget).st_size

    @staticmethod
    def FolderExist(sTarget : str) -> bool :
        return path.exists(sTarget)

    @staticmethod
    def CreateFolder(sFolder : str) :
        if not path.exists(sFolder) :
            os.mkdir(sFolder)

    @staticmethod
    def RemoveFolder(sFolder : str) :
        if path.exists(sFolder) :
	        shutil.rmtree(sFolder, ignore_errors = True)

    @staticmethod
    def RemoveFile(sFilePath : str) :
        if path.isfile(sFilePath) :
            os.remove(sFilePath)

    def _Kill(self) :
        self.m_sPath = None
        self.m_sName = None
        self.m_sFullName = None
        self.m_sExt = None
        
        self.m_fTarget = None

    def GetPath(self) -> str:
        return self.m_sPath

    def Raw(self) :
        return self.m_fTarget

    def Open(self, sTarget : str, sMode : str) -> SqlError :

        try :
            
            self.m_sPath, self.m_sName, self.m_sExt = Gen.SplitFileName(sTarget)
            self.m_sFullName = self.m_sName + self.m_sExt

            if not path.exists(self.m_sPath) :
                return SqlError(enum_Error.eNotFound, "Folder not found")

            if sMode[0] == 'r':
                if path.isfile(sTarget) == False :
                    return SqlError(enum_Error.eNotFound, "File not found")

            fTarget = open(sTarget, sMode)

            self.m_fTarget = fTarget

            return SqlError()

        except IOError as oErrInfo :

            return SqlError(enum_Error.eIOError,  f'winError {oErrInfo.winerror} : {oErrInfo.strerror}')

  
    def Close(self):

        if self.m_fTarget != None:
            self.m_fTarget.close()
            self._Kill()

    def Read(self, iQntByte : int = None) -> bytes:

        return self.m_fTarget.read(iQntByte)
    
    def Write(self, oData) -> int:

        return self.m_fTarget.write(oData)

    def Seek(self, iOffset : int, iModo : int):
        
        self.m_fTarget.seek(iOffset, iModo)