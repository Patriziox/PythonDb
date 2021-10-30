from enum import Enum, unique
from SqlGlobals import enum_DataType

@unique
class enum_SqlSchemaType(Enum):
    eEmpty = 0
    eColumn = 1
    
    eExpression = -4
    eCase = -3
    eAggregate = -2
    eRollUp = -1


class SqlSchemaItem :

    def __init__(self, eType : enum_SqlSchemaType, iTableUid : int, iColumn : int, iIndex : int, eDataType : enum_DataType, sTableName : str = None, sColName  : str = None, sAlias : str = None) :

        self.m_eType        = eType
        self.m_iTableUid    = iTableUid
        self.m_iColumn      = iColumn
        self.m_iIndex       = iIndex
        self.m_eDataType    = eDataType
        
        self.m_sTableName   = sTableName
        self.m_sColName     = sColName
        self.m_sAlias       = sAlias

    def __eq__(self, item : 'SqlSchemaItem') -> bool:
        if not isinstance(item , SqlSchemaItem) :
            return False

        return (self.m_iTableUid == item.m_iTableUid) and (self.m_iColumn == item.m_iColumn)

    def __ne__(self, item : 'SqlSchemaItem') -> bool:
        if not item :
            return True

        return (self.m_iTableUid != item.m_iTableUid) or (self.m_iColumn != item.m_iColumn)

    def GetType(self) -> enum_SqlSchemaType :
        return self.m_eType
    
    def GetTableUid(self) -> int :
        return self.m_iTableUid

    def GetColumn(self) -> int :
        return self.m_iColumn

    def GetIndex(self) -> int :
        return self.m_iIndex
    
    def GetDataType(self) -> enum_DataType :
        return self.m_eDataType
    
    def GetTableName(self) -> str :
        return self.m_sTableName

    def GetColName(self) -> str :
        return self.m_sColName

    def GetAlias(self) -> str :
        return self.m_sAlias
        
    
class SqlSchema :

    def __init__(self) :
        self.m_voSchema = []
		
    def __len__(self) :
        return len(self.m_voSchema)
		
    def __iter__(self) :
        return iter(self.m_voSchema)
    		
    def __getitem__(self, iIndex) :
        return self.m_voSchema[iIndex]

    def Append(self, oSchema : SqlSchemaItem) :
        self.m_voSchema.append(oSchema)
		
    def Extend(self, oSqlSchema : 'SqlSchema') :
        if oSqlSchema :
            self.m_voSchema.extends(oSqlSchema.m_voSchema)

    def GetSchema(self, sColName : str) -> SqlSchemaItem :
    	    
        if '.' in sColName :

            for oSchema in self.m_voSchema :

                sFullColName = f'{oSchema.GetTableName()}.{oSchema.GetColName()}'

                if sColName == sFullColName :
                    return oSchema
        else :

            for oSchema in self.m_voSchema :

                if (sColName == oSchema.GetColName()) or (sColName == oSchema.GetAlias()) :
                    return oSchema


        return None
   