from TCP_IP.TcpSocket import TcpFrame, enum_TcpToken
import pickle

import SqlGlobals as Glob

from SQL.SqlTable import *
from SQL.SqlSelect import *
from SQL.SqlTables import *

from SqlGeneric import SqlGeneric as Gen


class SqlQuery:
    
    st_tSqlCmmd = (
        ('CREATE TEMPORARY TABLE IF NOT EXISTS ', '_CreateTableHnd', (True, True, ) ),
        ('CREATE TEMPORARY TABLE ', '_CreateTableHnd', (False, True, ) ),
        ('CREATE TABLE IF NOT EXISTS ', '_CreateTableHnd', (True, False, ) ),
        ('CREATE TABLE ', '_CreateTableHnd', (False, False, ) ),
        ('INSERT INTO ', '_InsertIntoHnd', None),
        ('SELECT ', '_SelectHnd', None)
    )
    
    def __init__(self, oDataBase : SqlTables) :

        self.m_oDataBase = oDataBase

    def Parse(self, sQuery : str) :
        
        sQuery = Gen.Normalize(sQuery)
        sQuery, bResult = Gen.RemoveTonde(sQuery)

        for cmmd in self.st_tSqlCmmd:
            sBody = Gen.IsToken(sQuery, cmmd[0])

            if sBody is None :
                continue

            fuHandler = getattr(self, cmmd[1])

            if cmmd[2] :
                return fuHandler(sBody, cmmd[2])
                
            return fuHandler(sBody)

        oSqlError = SqlError(enum_Error.eSyntax, 'Not found any Command')

        return enum_TcpToken.eReplyFailure, oSqlError.Serialize()
    
    def _CreateTableHnd(self, sQuery, vPars) -> enum_TcpToken and None :
       
        if not isinstance(vPars[0], bool) :
            return enum_TcpToken.eReplySyntaxError, None

        if not isinstance(vPars[1], bool) :
            return enum_TcpToken.eReplySyntaxError, None

        oTables = self.m_oDataBase.Tables()

        eResult, oTable = oTables.CreateTable(vPars[0], vPars[1], sQuery)

        if(eResult == Glob.enum_TcpToken.eReplySuccessful):
            return enum_TcpToken.eReplySuccessful, None

        if(eResult == Glob.enum_TcpToken.eReplyOverWrite) :
            oTable.Save()
            return enum_TcpToken.eReplySuccessful, None

        return enum_TcpToken.eReplyFailure, None
    
    def _InsertIntoHnd(self, sQuery) -> enum_TcpToken and None :

        sHead, vParams, bResult = Gen.Split(sQuery, ' VALUES ')

        if not bResult :
            return enum_TcpToken.eReplySyntaxError, None

        sName, vCols, sCoda = Gen.FunctionParser(sHead)
        
        if(sName == None): #no colonne
            sName = sHead
            vCols = []
        
        elif sCoda != '' :
            return enum_TcpToken.eReplySyntaxError, None

        oTables = self.m_oDataBase.GetTables()

        oTable = oTables.GetTable(sName)

        if not oTable :
            return enum_TcpToken.eReplySyntaxError, None
       
        sName, vVals, sCoda = Gen.FunctionParser('NOP' + vParams)

        if sCoda != '' :
            return enum_TcpToken.eReplySyntaxError, None

        bResult = oTable.InsertInto(vCols, vVals)

        return enum_TcpToken.eReplySuccessful if bResult else enum_TcpToken.eReplyFailure, None

    def _SelectHnd(self, sQuery) :
        
        oSelect = SqlSelect(self.m_oDataBase.Tables())

        oSqlTuples = oSelect.Parse(sQuery)

        if oSqlTuples :
            return enum_TcpToken.eReplyTuple, oSqlTuples.Serialize()
        
        oSqlError = oSelect.GetError()

        if not oSqlError :
            oSqlError = SqlError(enum_Error.eUnspecified, '???')

        return enum_TcpToken.eReplyFailure, oSqlError.Serialize()


        

