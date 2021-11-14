import pickle

from SqlGlobals import enum_DataType
from SQL.SqlSchema import *

class SqlColKeys:

    def __init__(self) -> None:
        self.m_doColKeys = dict()

    def __iter__(self) :
        return iter(self.m_doColKeys.values())

    def Update(self, sColName : str, iTableUid : int, iColIndex : int, eDataType : enum_DataType) -> None:
        self.m_doColKeys.update({sColName : SqlSchemaItem(enum_SqlSchemaType.eColumn, iTableUid, iColIndex, None, eDataType) })
        
    def Delete(self, sColName : str) -> None :
        
        self.m_doColKeys.pop(sColName)
                        
        for item, iIndex in enumerate(self.m_doColKeys) :
            item.SetColumn(iIndex)
        

    def GetKeys(self, sColName : str) -> SqlSchema:
        return self.m_doColKeys.get(sColName)

    def SetTabUid(self, iTableUid : int) :

        doColKeys = dict()
        
        item : SqlSchema

        for key, item in self.m_doColKeys.items() :
            
            doColKeys.update({key : SqlSchemaItem(enum_SqlSchemaType.eColumn, iTableUid, item.GetColumn(), item.GetIndex(), item.GetType())})

        self.m_doColKeys = doColKeys

class SqlTableKeys:

    def __init__(self) -> None:
        self.m_doTableKeys = dict()

    def Update(self, sTableName : str, oColKeys : SqlColKeys) -> None:
        self.m_doTableKeys.pop(sTableName, None)
        self.m_doTableKeys.update({sTableName : oColKeys})

    def GetTableKeys(self, sTableName) -> SqlColKeys:
        return self.m_doTableKeys.get(sTableName)

    def GetKeys(self, sFullColName : str) -> SqlSchemaItem:

        vSplit = sFullColName.split('.')

        if len(vSplit) == 2 : # tab.col
            
            oColKeys = self.m_doTableKeys.get(vSplit[0])

            if not oColKeys :
                return None

            return oColKeys.GetKeys(vSplit[1])

        voColKeys = self.m_doTableKeys.values()

        oResult = None

        for oColKeys in voColKeys :

            oSqlSchema = oColKeys.GetKeys(vSplit[0])

            if not oSqlSchema :
                continue
            
            if not oResult :
                return None

            oResult = oSqlSchema
            break
        
        return oResult