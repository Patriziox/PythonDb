from SqlGeneric import SqlGeneric as Gen
from SQL.SqlColKeys import *
from SQL.SqlExpression import *

class SqlCase :

    def __init__(self) :
        self.m_sAlias = ''
        self.m_oExp = None
        self.m_voExp = []
        self.m_oError = SqlError()

    def GetAlias(self) -> str :
        return self.m_sAlias

    def GetError(self) -> str :
        return self.m_oError

    def Parse(self, sQuery : str) -> bool:
        
        sHead, sFoot, bResult = Gen.Split(Gen.RemoveTonde(sQuery)[0], 'WHEN ')
        
        sHead = sHead.strip()

        if sHead :
            self.m_oExp = sHead
                
        sHead, sAlias, bResult = Gen.Split(sFoot, ' AS ')

        if bResult :
            self.m_sAlias = sAlias.strip()

        sHead, sFoot, bResult = Gen.Split(sHead, ' END')

        if not bResult :
            self.m_oError = SqlError(enum_Error.eSyntax, "'CASE' statement must be closed by 'END' keyword")
            return False

        sFoot, sElse, bResult = Gen.Split(sHead, ' ELSE ')

        if bResult :
            sElse = sElse.strip()

        while sFoot :
            
            sHead, sFoot, bResult = Gen.Split(' ' + sFoot, ' WHEN ')

            sFoot = sFoot.strip()

            if sFoot and not bResult :
                self.m_oError = SqlError(enum_Error.eSyntax, "Not found 'WHEN' keyword in 'CASE' statement")
                return False

            sExp, sResult, bResult = Gen.Split(sHead, ' THEN ')

            if not (bResult and sExp and sResult) :
                self.m_oError = SqlError(enum_Error.eSyntax, "Not found 'THEN' keyword in 'CASE' statement")
                return False

            self.m_voExp.append((sExp.strip(), sResult.strip(),))
            

        if sElse :
            self.m_voExp.append(('1', sElse,))

        return True

    def SetValue(self, oSchema : SqlSchema) -> bool:
        
        if self.m_oExp :
            
            oExp = SqlExp()

            if not oExp.Parse(self.m_oExp, oSchema) :
                self.m_oError = oExp.GetError()
                return False

            self.m_oExp = oExp

        voExp = [] 

        for tExp in self.m_voExp :

            oExp = SqlExp()

            if not oExp.Parse(tExp[0], oSchema) :
                self.m_oError = oExp.GetError()
                return False

            oExp.SetResult(Gen.GetString(tExp[1]))

            voExp.append(oExp)
        
        self.m_voExp = voExp

        return True

    def Evalute(self, vValue : list) :
        
        if self.m_oExp :
            
            anyValue1 = self.m_oExp.Evalute(vValue) 

            if anyValue1 == None :
                return None
            
            for oExp in self.m_voExp :
           
                anyValue2 = oExp.Evalute(vValue) 
                
                if anyValue2 == None :
                    return None
                
                if anyValue1 == anyValue2 :
                    return oExp.GetResult()

        else :   

            for oExp in self.m_voExp :
            
                anyValue = oExp.Evalute(vValue) 
                
                if anyValue :
                    return oExp.GetResult()
        
        return 'NULL'

