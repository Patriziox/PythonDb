from io import FileIO
import pickle

from SqlGeneric import SqlGeneric as Gen
from SQL.SqlExpression import *

class SqlCheck:

    def __init__(self):
        self.m_sExpression = ""
        self.m_oExp = None

    def Parse(self, sQuery: str, oTableKeys : SqlTableKeys):

        sExp, bResult = Gen.RemoveTonde(sQuery)

        self.m_sExpression = sExp.strip()
        
        self.m_oExp = SqlExp()

        self.m_oExp.Parse(self.m_sExpression, oTableKeys)

    def GetExpr(self):
        return self.m_sExpression
   
    def Clone(self) -> object:

        oCheck = SqlCheck()
        oCheck.Parse(self.m_sExpression)

        return oCheck

    def Evalute(self, vValue : list) ->bool :
        return self.m_oExp.Evalute(vValue)

    def Save(self, fTarget : FileIO) -> bytes:
       
        pickle.dump(self.m_sExpression, fTarget)
        return Gen.Crc(tuple(self.m_sExpression))

    def Load(self, fSource : FileIO, oTableKeys : SqlTableKeys) -> bytes:
       
        self.m_sExpression = pickle.load(fSource)

        self.m_oExp = SqlExp()

        self.m_oExp.Parse(self.m_sExpression, oTableKeys)

        return Gen.Crc(tuple(self.m_sExpression))