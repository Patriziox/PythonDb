from enum import Enum, unique, auto

import SqlGlobals as Glob
from SqlError import *
from SqlGeneric import SqlGeneric as Gen
from SQL.SqlColKeys import *
from SQL.SqlNull import SqlNull

NULL = SqlNull()

@unique
class enum_AggregateFunc(Enum):
    eCount = 0
    eSum = 1
    eAvg = 2
    eMin = 3
    eMax = 4

class enum_ExpType(Enum):
    
    eAnd = 0
    eOr = auto()
    eNotIn = auto()
    eIn = auto()
    eNot = auto()
    

    eNotNull = auto()
    eNull = auto()
       
    eEqu = auto()
    eNoEqu = auto()
    eGreatEqu = auto()
    eGreat = auto()
    eLessEqu = auto()
    eLess = auto()
    eNotBetween = auto()
    eBetween = auto()
    
    eMolty = auto()
    eDiv = auto()
    eSum = auto()
    eSub = auto()
   
    eLen = auto()
    eCount = auto()
    eAvg = auto()
    eCase = auto()
        
    eNone = 100
    eValue = auto()
    eLiteral = auto()
    eAllNotNull = auto()
    eAll = auto()
    eSubQuery = auto()
    eSubQueryMulti = auto()
    

class SqlExpNode :

    st_tExpOper = (
        (' SELECT ', enum_ExpType.eSubQuery, 0),
        (' && ', enum_ExpType.eAnd),
        (' || ', enum_ExpType.eOr),
        (' NOT IN ', enum_ExpType.eNotIn),
        (' IN ', enum_ExpType.eIn),
        (' ! ', enum_ExpType.eNot),
        (' IS NOT NULL ', enum_ExpType.eNotNull),
        (' IS NULL ', enum_ExpType.eNull),
        (' = ', enum_ExpType.eEqu),
        (' <> ', enum_ExpType.eNoEqu),
        (' >= ', enum_ExpType.eGreatEqu),
        (' > ', enum_ExpType.eGreat),
        (' <= ', enum_ExpType.eLessEqu),
        (' < ', enum_ExpType.eLess),
        (' NOT BETWEEN ', enum_ExpType.eNotBetween),
        (' BETWEEN ', enum_ExpType.eBetween),
        (' * ', enum_ExpType.eMolty),
        (' / ', enum_ExpType.eDiv),
        (' + ', enum_ExpType.eSum),
        (' - ', enum_ExpType.eSub),
        (' LEN ', enum_ExpType.eLen),
        (' COUNT ', enum_ExpType.eCount, enum_AggregateFunc.eCount),
        (' AVG ', enum_ExpType.eAvg, enum_AggregateFunc.eAvg),
        (' CASE ', enum_ExpType.eCase, ),
    )

    def __init__(self) -> None:
        self.m_eType = enum_ExpType.eNone
        self.m_eDataType = enum_DataType.eNone
        self.m_pLeft    = None
        self.m_pRight   = None
        self.m_oValue   = None
        self.m_oItem    = None
        self.m_sText    = None
        self.m_eAggrType = None
        
    
    def GetType(self) -> enum_ExpType :
        return self.m_eType

    def GetAggrType(self) -> enum_AggregateFunc :
        return self.m_eAggrType

    def SetItem(self, oItem) :
        self.m_oItem = oItem
    
    def GetText(self) -> str :
        return self.m_sText

    def Parse(self, oParent : 'SqlExpNode', sExpressione : str, vNodeList : list = None) :
        
        sExpr, bParentesiTonde = Gen.RemoveTonde(sExpressione)

        sExpr = f" {sExpr} "

        for vToken in SqlExpNode.st_tExpOper:
            
            sHead, sFoot, bResult = Gen.Split(sExpr, vToken[0])

            if not bResult :
                continue
            
            self.m_eType = vToken[1]

            if self.m_eType in (enum_ExpType.eCount, enum_ExpType.eAvg, ) :
                self.m_eAggrType = vToken[2]
                self.m_sText = sExpr.strip() 
			
            elif self.m_eType == enum_ExpType.eSubQuery :
               
                if not bParentesiTonde : 
                    return None

                sHead, sFoot, bResult = Gen.Split(sExpr, ' SELECT ')

                if not bResult :
                    return None
                
                self.m_sText = sFoot.strip() 

                sFoot = ''

                if isinstance(oParent.m_oValue, list) : 

                    oParent.m_oValue = []
                    self.m_eType = enum_ExpType.eSubQueryMulti

            elif self.m_eType == enum_ExpType.eCase :
                
                self.m_sText, bParentesiTonde = Gen.RemoveTonde(sFoot)
                
                if not bParentesiTonde : 
                    return None

                sFoot = ''
               
            elif self.m_eType == enum_ExpType.eBetween :
				
                sVal1, sVal2, bResult = Gen.Split(sFoot, ' AND ')
				
                if not bResult :
                    return None
												
                self.m_eType = enum_ExpType.eAnd

                sFoot = f'({sHead} <= {sVal2})' 
                sHead = f'({sHead} >= {sVal1})' 

            elif self.m_eType == enum_ExpType.eNotBetween :
				
                sVal1, sVal2, bResult = Gen.Split(sFoot, ' AND ')
				
                if not bResult :
                    return None
												
                self.m_eType = enum_ExpType.eOr

                sFoot = f'({sHead} > {sVal2})' 
                sHead = f'({sHead} < {sVal1})' 
			
            if sHead != '' :
                oNodo = SqlExpNode()
                self.m_pLeft = oNodo.Parse(self, sHead, vNodeList)

                if not self.m_pLeft :
                    return None

            if self.m_eType in (enum_ExpType.eIn, enum_ExpType.eNotIn) :
                
                self.m_oValue = []

                if Gen.BeginsWith(sFoot, '( SELECT ') :

                    oNodo = SqlExpNode()
                    self.m_pRight = oNodo.Parse(self, sFoot, vNodeList)
                
                else : 

                    sNop1, vsValues, sNop2 = Gen.FunctionParser('NOP' + sFoot)
                
                    for sValue in vsValues:
                        oNodo = SqlExpNode()
                    
                        self.m_oValue.append(oNodo.Parse(self, ' ' + sValue, vNodeList))
                            
            elif sFoot != '' :

                oNodo = SqlExpNode()
                self.m_pRight = oNodo.Parse(self, sFoot, vNodeList)

                if not self.m_pRight :
                    return None
            
            if vNodeList != None:
                vNodeList.append(self)
            
            return self
               
        oItem = sHead.strip()

        self.m_eType = enum_ExpType.eLiteral
        
        if(oItem.isnumeric() == True):
            self.m_oValue = int(oItem)
            self.m_eDataType = enum_DataType.eInt
        
        elif (oItem[0] == '"') and (oItem[-1] == '"'):
            self.m_oValue = str(oItem)
            self.m_eDataType = enum_DataType.eVarchar

        else :

            sHead, sFoot, bResult = Gen.Split(oItem, '.')

            if bResult :

                if not Gen.IsName(sFoot) :
                    return None
          
            if not Gen.IsName(sHead) :
                return None

            self.m_oItem = oItem
            self.m_eType = enum_ExpType.eValue

        if vNodeList != None :
            vNodeList.append(self)

        return self

    def _getColumns(self, sExpr : str, vsColList : list) :

        sExpr, bResult = Gen.RemoveTonde(sExpr)
        
        sExpr = f" {sExpr} "

        for vToken in SqlExpNode.st_tExpOper:
            
            sHead, sFoot, bResult = Gen.Split(sExpr, vToken[0])

            if bResult :
            
                if sHead != '' :
                    self._getColumns(sHead, vsColList)
                                        
                if sFoot != '' :

                    self._getColumns(sFoot, vsColList)
            
                return
               
        sColName = sExpr.strip()
 
        if sColName.isnumeric() == True :
            return 
        
        if (sColName[0] == '"') and (sColName[-1] == '"') :
            return
        
        vsColList.append(sColName)

class SqlExp:

    st_vExpOperHnd = [
        '', # and
        '', # or
        '_NotInHnd',
        '_InHnd',
        '_NotHnd',
        '', # Not Null
        '', # Null
        '_EquHnd',
        '_NoEquHnd',
        '_GreatEquHnd',
        '_GreatHnd',
        '_LessEquHnd',
        '_LessHnd',
        '',# not between
        '',# between
        '_MoltyHnd',
        '_DivHnd',
        '_SumHnd',
        '_SubHnd',
        '_LenHnd',
    ]

    def __init__(self, sExp : str = None) -> None:
        self.m_sExp         = sExp
        self.m_oRoot        = None
        self.m_vValues      = None
        self.m_vNodeList    = None
        self.m_eError       = None
        self.m_Result       = None
    
    def _typeHnd(self, oPar1, oPar2) :

        oType1 = type(oPar1)

        if oType1 is int :
            return int(oPar2)

        return oPar2

    def _LenHnd(self, oPar) -> int:
        
        if isinstance(oPar, str) :
            return len(oPar) - 2
        
        return None

    def _LessHnd(self, oPar1, oPar2) -> bool:
        
        if oPar1 in (NULL, None) :
            return oPar1
			
        if oPar2 in (NULL, None) :
            return oPar2
        
        return oPar1 < self._typeHnd(oPar1, oPar2)
   
    def _LessEquHnd(self, oPar1, oPar2) -> bool:

        if oPar1 in (NULL, None) :
            return oPar1
			
        if oPar2 in (NULL, None) :
            return oPar2

        return oPar1 <= self._typeHnd(oPar1, oPar2)
    
    def _GreatHnd(self, oPar1, oPar2) -> bool:
        
        if oPar1 in (NULL, None) :
            return oPar1
			
        if oPar2 in (NULL, None) :
            return oPar2
        
        return oPar1 > self._typeHnd(oPar1, oPar2)

    def _GreatEquHnd(self, oPar1, oPar2) -> bool:
        
        if oPar1 in (NULL, None) :
            return oPar1
			
        if oPar2 in (NULL, None) :
            return oPar2
        
        return oPar1 >= self._typeHnd(oPar1, oPar2)

    def _EquHnd(self, oPar1, oPar2) -> bool:
        
        if oPar1 in (NULL, None) :
            return oPar1
			
        if oPar2 in (NULL, None) :
            return oPar2
        
        return oPar1 == self._typeHnd(oPar1, oPar2)

    def _NoEquHnd(self, oPar1, oPar2) -> bool:
    
        if oPar1 in (NULL, None) :
            return oPar1
			
        if oPar2 in (NULL, None) :
            return oPar2
        
        return oPar1 != self._typeHnd(oPar1, oPar2)
      
    def _NotHnd(self, oPar1, oPar2) -> bool:
        
        if oPar1 in (NULL, None) :
            return oPar1
		        
        return not oPar1

    def _InHnd(self, oThisValue, voValues) -> bool:
        
        return (oThisValue in voValues)
      
    def _NotInHnd(self, oThisValue, voValues) -> bool:
        
       return (oThisValue not in voValues)

    def _MoltyHnd(self, oPar1, oPar2) :

        if oPar1 in (NULL, None) :
            return oPar1
			
        if oPar2 in (NULL, None) :
            return oPar2

        return oPar1 * self._typeHnd(oPar1, oPar2)

    def _SumHnd(self, oPar1, oPar2) :

        if oPar1 in (NULL, None) :
            return oPar1
			
        if oPar2 in (NULL, None) :
            return oPar2

        return oPar1 + self._typeHnd(oPar1, oPar2)

    def Parse(self, sExp : str, oSchema : SqlSchema = None, bNodeList : bool = False) -> bool:
        
        if bNodeList :
            self.m_vNodeList = []
           
        self.m_oRoot = SqlExpNode()
        
        if not sExp :
            sExp = self.m_sExp

        if not sExp :
            self.m_eError = SqlError(enum_Error.eSyntax, '')
            return None
        
        if sExp.strip() == '*' :
            self.m_oRoot.m_eType = enum_ExpType.eAll
            return True

        if self.m_oRoot.Parse(None, sExp, self.m_vNodeList) == None :
            
            self.m_eError = SqlError(enum_Error.eSyntax, '')
            return None

        if(oSchema != None):
           if not self.SetItem(self.m_oRoot, oSchema) :
               return False
        
        return True
   
    def _evalute(self, oNodo : SqlExpNode, vvValue : list):

        if oNodo == None :
            return None

        if(oNodo.m_eType == enum_ExpType.eValue):
            
            if not isinstance(oNodo.m_oItem, tuple) :
                return None

            if(vvValue[oNodo.m_oItem[1]][oNodo.m_oItem[2]] == NULL):
                oValue = NULL
            
            elif oNodo.m_eDataType == enum_DataType.eInt:
                oValue = int(vvValue[oNodo.m_oItem[1]][oNodo.m_oItem[2]])

            elif oNodo.m_eDataType == enum_DataType.eVarchar:
                oValue = f'"{str(vvValue[oNodo.m_oItem[1]][oNodo.m_oItem[2]], "UTF-8")}"'

            else: oValue = vvValue[oNodo.m_oItem[1]][oNodo.m_oItem[2]]

            if self.m_vValues != None :
                self.m_vValues.append(oValue)
            
            return oValue

        if oNodo.m_eType == enum_ExpType.eLiteral :
            return oNodo.m_oValue

        if oNodo.m_eType == enum_ExpType.eCase :
            return oNodo.m_oItem.Evalute(vvValue)

        if oNodo.m_eType == enum_ExpType.eAnd :
            
            oResult1 = self._evalute(oNodo.m_pLeft, vvValue)

            if oResult1  in (False, None):
                return oResult1
            
            oResult2 = self._evalute(oNodo.m_pRight, vvValue)

            if oResult2  in (False, None):
                return oResult2

            return True if (oResult1 == True) and (oResult2 == True) else NULL

        if(oNodo.m_eType == enum_ExpType.eOr) :
            
            oResult1 = self._evalute(oNodo.m_pLeft, vvValue)

            if oResult1  in (True, None):
                return oResult1

            oResult2 = self._evalute(oNodo.m_pRight, vvValue)

            if oResult2  in (True, None):
                return oResult2

            return False if (oResult1 == False) and (oResult2 == False) else NULL

        if oNodo.m_eType == enum_ExpType.eNotNull :
			
            oResult = self._evalute(oNodo.m_pLeft, vvValue)
            
            if oResult == NULL :
                return False
						            
            return None if oResult == None else True

        if oNodo.m_eType == enum_ExpType.eNull :
            
            oResult = self._evalute(oNodo.m_pLeft, vvValue)
            
            if oResult == NULL :
                return True
						            
            return None if oResult == None else False

        if oNodo.m_eType == enum_ExpType.eIn :

            if oNodo.m_pRight and oNodo.m_pRight.GetType() in (enum_ExpType.eSubQuery, enum_ExpType.eSubQueryMulti) :
                return self._InHnd(self._evalute(oNodo.m_pLeft, vvValue), oNodo.m_pRight.m_oValue)

            return self._InHnd(self._evalute(oNodo.m_pLeft, vvValue), oNodo.m_oValue)

        if oNodo.m_eType == enum_ExpType.eNotIn :

            if oNodo.m_pRight and oNodo.m_pRigh.GetType() in (enum_ExpType.eSubQuery, enum_ExpType.eSubQueryMulti) :
                return self._NotInHnd(self._evalute(oNodo.m_pLeft, vvValue), oNodo.m_pRight.m_oValue)

            return self._NotInHnd(self._evalute(oNodo.m_pLeft, vvValue), oNodo.m_oValue)

        if oNodo.m_eType == enum_ExpType.eAllNotNull :
            return NULL not in vvValue

        if oNodo.m_eType == enum_ExpType.eAll :
            return True

        if oNodo.m_eType == enum_ExpType.eSubQuery  :
            return oNodo.m_oValue

        if oNodo.m_eType == enum_ExpType.eSubQueryMulti  :
            return oNodo.m_oValue

        if oNodo.m_eAggrType :
            oAggrValue = vvValue[-1]
            return oAggrValue[oNodo.m_oItem]
        
        Handler = getattr(self, SqlExp.st_vExpOperHnd[oNodo.m_eType.value])
        
        if not oNodo.m_pRight :
            return None

        if oNodo.m_pLeft :
            return Handler(self._evalute(oNodo.m_pLeft, vvValue), self._evalute(oNodo.m_pRight, vvValue))
        
        return Handler(self._evalute(oNodo.m_pRight, vvValue))
       
    def Evalute(self, vValue : list, bExportValue : bool = False) :
        
        if bExportValue :
            self.m_vValues = []

        vvValue = vValue if isinstance(vValue[0], list) else [vValue]

        return self._evalute(self.m_oRoot, vvValue)
        
    def SetItem(self, oNode : SqlExpNode, oSchema : SqlSchema) -> bool:

        if(oNode.m_oItem != None):

            sColName = oNode.m_oItem

            oSchemaItem = oSchema.GetSchema(sColName)

            if oSchemaItem != None :

                iTableIndex = None

                for item in oSchema :
                    # if isinstance(item, SqlSchema) :
                    if item.GetTableUid() == oSchemaItem.GetTableUid() :
                        iTableIndex = item.GetIndex()
                        break
                else :

                    return False

                oNode.m_oItem = (sColName, iTableIndex, oSchemaItem.GetColumn())
                oNode.m_eDataType = oSchemaItem.GetType()
            
            else :

                return False

        if(oNode.m_pLeft != None):
            if not self.SetItem(oNode.m_pLeft, oSchema) :
                return False

        if(oNode.m_pRight != None):
            if not self.SetItem(oNode.m_pRight, oSchema) :
                return False
        
        return True
    
    def GetValues(self) -> list :
        return self.m_vValues

    def GetResult(self) -> list :
        return self.m_Result

    def GetNodes(self) -> list :
        return self.m_vNodeList

    def GetError(self) -> SqlError :
        return self.m_eError

    def GetSubQuery(self) -> list :
        return self.m_vNodeSub

    def SetNotNull(self) :
        self.m_oRoot = SqlExpNode()
        self.m_oRoot.m_eType = enum_ExpType.eAllNotNull
  
    def GetColumns(self, sExp : str = None) -> list :
        
        vsColList = []
        
        if sExp :

            oRoot = SqlExpNode()

            oRoot._getColumns(sExp, vsColList)

        else :

            for oNode in self.m_vNodeList :
                if oNode.GetType() == enum_ExpType.eValue :
                    vsColList.append(oNode.oItem)

        return vsColList

    def SetResult(self, oResult) :
        self.m_Result = oResult