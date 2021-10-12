from SqlGlobals import enum_DataType

class SqlSchemaItem :

    def __init__(self, iTableUid : int, iColumn : int, iIndex : int, eType : enum_DataType, sTableName  : str = None, sColName  : str = None, sAlias : str = None) :

        self.m_iTableUid = iTableUid
        self.m_iColumn = iColumn
        self.m_iIndex = iIndex
        self.m_eType = eType
        
        self.m_sTableName = sTableName
        self.m_sColName = sColName
        self.m_sAlias = sAlias

    def GetTableUid(self) -> int :
        return self.m_iTableUid

    def GetColumn(self) -> int :
        return self.m_iColumn

    def GetIndex(self) -> int :
        return self.m_iIndex
    
    def GetType(self) -> enum_DataType :
        return self.m_eType
    
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