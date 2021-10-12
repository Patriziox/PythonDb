from genericpath import isfile
from os import path
import pickle
import SqlGlobals as Glob


class SqlUniqueValues:

    def __init__(self) -> None:
        self.m_setValues = set()
   
    def Add(self, oValue) -> None:
       
        self.m_setValues.add(oValue)

    def Check(self, oValue) -> bool:
        return oValue in self.m_setValues

class SqlUnique:

    def __init__(self, sTabName : str) -> None:
        self.m_bModified = False
        self.m_sName = sTabName
        self.m_dColumns = dict()

    def Check(self, sColumn : str, oValue) -> bool:
        
        oUniqueValues = self.m_dColumns.get(sColumn)

        if(oUniqueValues == None):
            return False

        return oUniqueValues.Check(oValue)

    def Add(self, sColumn : str, oValue) -> None:

        oUniqueValues = self.m_dColumns.get(sColumn)

        if(oUniqueValues == None):

            oUniqueValues = SqlUniqueValues()
            
            self.m_dColumns.update({sColumn : oUniqueValues})


        oUniqueValues.Add(oValue)
        
        self.m_bModified = True
        
    def Load(self) -> bool:

        sFullPath = f"{Glob.TAB_FOLDER}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_UNIQUE_EXT}"

        if(path.isfile(sFullPath) == False):
            return False
        
        fSource = open(sFullPath, "rb")
       
        self.m_dColumns = pickle.load(fSource)

        fSource.close()
        
        self.m_bModified = False

        return True

    def Save(self) -> bool:
        
        if(self.m_bModified == False):
            return True

        sFullPath = f"{Glob.TAB_FOLDER}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_UNIQUE_EXT}"

        fTarget = open(sFullPath, "wb")

        pickle.dump(self.m_dColumns, fTarget)

        fTarget.close()

        self.m_bModified = False

        return True