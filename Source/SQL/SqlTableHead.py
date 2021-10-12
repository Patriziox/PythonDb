import pickle
from copy import deepcopy

import SqlGlobals as Glob
from FileSystem import FileSystem
from SqlGeneric import SqlGeneric as Gen
from SQL.SqlRecordHead import *

from SQL.SqlColKeys import *

class SqlTableHead:

    def __init__(self, sName, iQntAutoIncValue : int = 0)-> None:
   
        self.m_sName            = sName
        self.m_iIndex           = -1
        self.m_oColKeys         = SqlColKeys()
        self.m_vAutoIncValue    = ((1,) * iQntAutoIncValue)
        self.m_iQntNullCols     = 0

    def SetIndex(self, iIndex : int) :
        self.m_iIndex = iIndex

        self.m_oColKeys.SetTabUid(iIndex)

    def SetAutoIncValue(self, viValue : list):
        self.m_vAutoIncValue = viValue
        
    def GetAutoIncValues(self) -> tuple:
        return self.m_vAutoIncValue

    def GetKeys(self) -> SqlColKeys:
        return self.m_oColKeys

    def SetKeys(self, oColKeys : SqlColKeys) :
        self.m_oColKeys = oColKeys

    def GetQntNullCols(self) -> int:
        return self.m_iQntNullCols

    def InitNull(self, iQntNullCols : int) :
        self.m_iQntNullCols = iQntNullCols

    def Save(self, sFolder : str) -> bool:
        
        try :
                        
            fTarget = open(f"{sFolder}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_HEAD_EXT}", "wb")

            byData = pickle.dumps(self)

            oCrc = Gen.CrcBytes(byData)
            
            pickle.dump(oCrc, fTarget)
            pickle.dump(byData, fTarget)
                        
            fTarget.close()

            return True
        
        except :

            return False


    def Load(self, sFolder : str) -> bool:
        
        try :

            with FileSystem() as oFileTable :

                if oFileTable.Open(f"{sFolder}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_HEAD_EXT}", "rb") :
                    return False
                            
                oCrc1 = pickle.load(oFileTable.Raw())
                
                byData = pickle.load(oFileTable.Raw())

                oCrc2 = Gen.CrcBytes(byData)

                if oCrc1 != oCrc2 :
                    return False
                
                oSqlTableHead : SqlTableHead

                oSqlTableHead = pickle.loads(byData)

                self.m_sName            = oSqlTableHead.m_sName
                self.m_iIndex           = oSqlTableHead.m_iIndex
                self.m_oColKeys         = oSqlTableHead.m_oColKeys
                self.m_vAutoIncValue    = oSqlTableHead.m_vAutoIncValue
                self.m_iQntNullCols     = oSqlTableHead.m_iQntNullCols
                
                return True

        except :

            return False
    
    
    