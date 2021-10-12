import glob

import SqlGlobals as Glob
from SqlGeneric import SqlGeneric as Gen

from SQL.SqlTable import *
from SQL.SqlTableHead import *
from SQL.SqlColKeys import *

class SqlTables:

    def __init__(self) -> None:
        
        self.m_sRoot = ""
        self.m_voTables = []
        self.m_vsTableName = []
        self.m_oTableKeys = SqlTableKeys()
               
    def __iter__(self):
        return iter(self.m_voTables)

    def Initialize(self, sRoot : str) :
        
        self.m_sRoot = sRoot

        sKey = f"{self.m_sRoot}\\{Glob.TABLES_FOLDER}\\{Glob.TABLE_PREFIX}*{Glob.TABLE_HEAD_EXT}"

        vsTabHead = glob.glob(sKey)

        self.m_voTables = [None] * len(vsTabHead)

        for sTabHead in vsTabHead:
            
            sPath, sFilename, sExt = Gen.SplitFileName(sTabHead)

            sTableName = sFilename.replace(Glob.TABLE_PREFIX, '', 1)
            
            if(sTableName in self.m_voTables):
                continue

            oTabHead = SqlTableHead(sTableName)

            if not oTabHead.Load(f'{self.m_sRoot}\\{Glob.TABLES_FOLDER}') :
                return False
            
            self.m_vsTableName.append(sTableName)
            
            self.m_oTableKeys.Update(sTableName, oTabHead.GetKeys())



    def GetTableKeys(self) -> SqlTableKeys:
        return self.m_oTableKeys

    def GetTableNames(self) -> list:
        return self.m_vsTableName

    def CreateTable(self, bIfNotExist : bool, bTemporary : bool, sQuery : str) -> Glob.enum_TcpToken and SqlTable:

        oTable = SqlTable(f'{self.m_sRoot}\\{Glob.TABLES_FOLDER}', bTemporary)
        
       
        if not oTable.Parse(sQuery) :
            return Glob.enum_TcpToken.eFailure, None
        
        sTabName = oTable.GetName()
                      
        for sTabella in self.m_vsTableName:
            if sTabella == sTabName :
                                
                if bIfNotExist :
                    return Glob.enum_TcpToken.eNone, None

                oTabella = SqlTable(f'{self.m_sRoot}\\{Glob.TABLES_FOLDER}')

                oTabella.Load(sTabName)

                iTabIndex = oTabella.GetIndex()

                oTable.SetIndex(iTabIndex)

                oTable.Drop()

                self.m_oTableKeys.Update(sTabName, oTable.m_oHead.GetKeys())

                self.m_voTables[iTabIndex] = oTable

                return Glob.enum_TcpToken.eReplyOverWrite, oTable

        oTable.SetIndex(len(self.m_vsTableName))

        if sTabName not in self.m_vsTableName :
            self.m_vsTableName.append(sTabName) 

        self.m_oTableKeys.Update(sTabName, oTable.m_oHead.GetKeys())
               
        self.m_voTables.append(oTable)

        if oTable.Save() :
            return Glob.enum_TcpToken.eReplySuccessful, oTable

        return Glob.enum_TcpToken.eReplyFailure, None
        
    def GetTable(self, sName : str) -> SqlTable :
        
        for oTable in self.m_voTables :
            if oTable :
                if oTable.GetName() == sName :
                    return oTable
        
        oTable = SqlTable(f'{self.m_sRoot}\\{Glob.TABLES_FOLDER}')

        if not oTable.Load(sName) :
            return None

        iQntTables = len(self.m_voTables)

        if iQntTables <= oTable.GetIndex() :
            self.m_voTables.extend([None] * (oTable.GetIndex() - iQntTables + 1))

        self.m_voTables[oTable.GetIndex()] = oTable

        self.m_oTableKeys.Update(oTable.GetName(), oTable.m_oHead.GetKeys())
        
        return oTable
   