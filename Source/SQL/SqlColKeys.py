import pickle

from SqlGlobals import enum_DataType
from SQL.SqlSchema import *

class SqlColKeys:

    def __init__(self) -> None:
        self.m_doColKeys = dict()
    
    def Update(self, sColName : str, iTableUid : int, iColIndex : int, eDataType : enum_DataType) -> None:
        self.m_doColKeys.update({sColName : SqlSchemaItem(iTableUid, iColIndex, None, eDataType) })

    def GetKeys(self, sColName : str) -> SqlSchema:
        return self.m_doColKeys.get(sColName)

    def SetTabUid(self, iTableUid : int) :

        doColKeys = dict()
        
        item : SqlSchema

        for key, item in self.m_doColKeys.items() :
            
            doColKeys.update({key : SqlSchemaItem(iTableUid, item.GetColumn(), item.GetIndex(), item.GetType())})

        self.m_doColKeys = doColKeys

    # def Serialize(self) -> bytes:
        
    #     pickle.dumps(self.m_doColKeys)
    #     # return self.m_doColKeys

    # def Unserialize(self, doColKeys : dict) -> None:
    #     self.m_doColKeys = doColKeys

class SqlTableKeys:

    def __init__(self) -> None:
        self.m_doTableKeys = dict()

    def Update(self, sTableName : str, oColKeys : SqlColKeys) -> None:
        self.m_doTableKeys.pop(sTableName, None)
        self.m_doTableKeys.update({sTableName : oColKeys})

    def GetKeys(self, sFullColName : str) -> SqlSchema:

        vSplit = sFullColName.split('.')

        if(len(vSplit) == 2): # tab.col
            
            oColKeys = self.m_doTableKeys.get(vSplit[0])

            if(oColKeys == None):
                return None

            return oColKeys.GetKeys(vSplit[1])

        voColKeys = self.m_doTableKeys.values()

        oResult = None

        for oColKeys in voColKeys :

            oSqlSchema = oColKeys.GetKeys(vSplit[0])

            if(oSqlSchema == None):
                continue
            
            if(oResult != None):
                return None

            oResult = oSqlSchema
            break
        
        return oResult