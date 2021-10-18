from SQL.SqlExpression import *

class SqlAggregateItem :

    def __init__(self) :
        
        self.m_vValue    = []
        self.m_viQntRow  = []
     
    def Evalute(self, eAggrFunc : enum_AggregateFunc, oExp : SqlExp, bDistinct : bool, vValue : list, iIndex : int) :
            
        if iIndex >= len(self.m_viQntRow) :
          
            self.m_viQntRow.append(0)

            if eAggrFunc == enum_AggregateFunc.eCount :
                
                self.m_vValue.append(0)

                if bDistinct :
                    self.m_vItems = []
           
            elif eAggrFunc in (enum_AggregateFunc.eMin, enum_AggregateFunc.eMax) :
                self.m_vValue.append(None)
            else : 
                self.m_vValue.append(0)

        oResult = oExp.Evalute(vValue, bDistinct)

        match eAggrFunc :

            case enum_AggregateFunc.eCount :
                if oResult :
                    
                    if bDistinct :
                        
                        vValori = self.m_oExp.GetValues()

                        if vValori not in self.m_vItems :
                            self.m_vItems.append(vValori)
                            self.m_viQntRow[iIndex] += 1

                    else : 
                        self.m_viQntRow[iIndex] += 1

            case enum_AggregateFunc.eSum :
                
                if isinstance(oResult, (int,  float)) :
                    self.m_vValue[iIndex] += oResult

            case enum_AggregateFunc.eAvg :
            
                if isinstance(oResult, (int,  float)) :
                    self.m_vValue[iIndex] += oResult
                    self.m_viQntRow[iIndex] += 1

            case enum_AggregateFunc.eMin :
            
                if isinstance(oResult, (int,  float)) :
                    if (self.m_vValue[iIndex] == None) or (oResult < self.m_vValue[iIndex]) :
                        self.m_vValue[iIndex] = oResult
        
            case enum_AggregateFunc.eMax :
            
                if isinstance(oResult, (int,  float)) :
                    if (self.m_vValue[iIndex] == None) or (oResult > self.m_vValue[iIndex]) :
                        self.m_vValue[iIndex] = oResult

    def GetValue(self, eAggrFunc : enum_AggregateFunc, iIndex : int) :
        
        oResult = None

        match eAggrFunc :

            case enum_AggregateFunc.eCount :
                oResult = self.m_viQntRow[iIndex]

            case (enum_AggregateFunc.eSum | enum_AggregateFunc.eMin | enum_AggregateFunc.eMax) :
                oResult = self.m_vValue[iIndex]

            case enum_AggregateFunc.eAvg :
                oResult = self.m_vValue[iIndex] / self.m_viQntRow[iIndex]
       
        return oResult
        


class SqlAggregate :
    
    def __init__(self, eAggrFunc : enum_AggregateFunc, sExp : str, bDistinct : bool = False):
        self._Kill()
      
        self.m_eAggrFunc = eAggrFunc
                
        self.m_oExp = SqlExp(sExp)
        
        self.m_bDistinct = bDistinct
        

    def _Kill(self) :
        
        self.m_sText      = None
        self.m_sAlias     = None
        self.m_oExp       = None
        self.m_iIndex     = None

        self.m_voAggregate = []

    def Parse(self, oSchema : SqlSchema) -> bool:
        return self.m_oExp.Parse(None, oSchema)

    def SetText(self, sText : str) :
        self.m_sText = sText

    def GetText(self) -> str :
        return self.m_sText

    def SetAlias(self, sAlias : str) :
        self.m_sAlias = sAlias

    def SetIndex(self, iIndex : int) :
        self.m_iIndex = iIndex

    def GetIndex(self) -> int :
        return self.m_iIndex

    def GetNodes(self) -> list :
        return self.m_oExp.GetNodes()
  
    def Evalute(self, vValue : list, iIndex : int, iSingleAggregate : int = 0) :

        lung = len(self.m_voAggregate)
        
        if iSingleAggregate >= lung :
            
            self.m_voAggregate.append(SqlAggregateItem())

            iSingleAggregate = lung
            
        self.m_voAggregate[iSingleAggregate].Evalute(self.m_eAggrFunc, self.m_oExp, self.m_bDistinct, vValue, iIndex)


    def GetValue(self, iIndex : int, iSingleAggregate : int = 0) :
        
        return self.m_voAggregate[iSingleAggregate].GetValue(self.m_eAggrFunc, iIndex)
