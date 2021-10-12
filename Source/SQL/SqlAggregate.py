from SQL.SqlExpression import *


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
        self.m_vValue     = []
        self.m_viQntRow   = []
        self.m_iIndex     = None

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
  
    def Evalute(self, vValue : list, iIndex : int) :
        
        if iIndex >= len(self.m_viQntRow) :
          
            self.m_viQntRow.append(0)

            if self.m_eAggrFunc == enum_AggregateFunc.eCount :
                
                self.m_vValue.append(0)

                if self.m_bDistinct :
                    self.m_vItems = []
           
            elif self.m_eAggrFunc in (enum_AggregateFunc.eMin, enum_AggregateFunc.eMax) :
                self.m_vValue.append(None)
            else : 
                self.m_vValue.append(0)

        oResult = self.m_oExp.Evalute(vValue, self.m_bDistinct)

        if self.m_eAggrFunc == enum_AggregateFunc.eCount :
            if oResult :
                
                if self.m_bDistinct :
                    
                    vValori = self.m_oExp.GetValues()

                    if vValori not in self.m_vItems :
                        self.m_vItems.append(vValori)
                        self.m_viQntRow[iIndex] += 1

                else : 
                    self.m_viQntRow[iIndex] += 1

        elif self.m_eAggrFunc == enum_AggregateFunc.eSum :
            
            if isinstance(oResult, (int,  float)) :
                self.m_vValue[iIndex] += oResult

        elif self.m_eAggrFunc == enum_AggregateFunc.eAvg :
            
            if isinstance(oResult, (int,  float)) :
                self.m_vValue[iIndex] += oResult
                self.m_viQntRow[iIndex] += 1

        elif self.m_eAggrFunc == enum_AggregateFunc.eMin :
            
            if isinstance(oResult, (int,  float)) :
                if (self.m_vValue[iIndex] == None) or (oResult < self.m_vValue[iIndex]) :
                    self.m_vValue[iIndex] = oResult
        
        elif self.m_eAggrFunc == enum_AggregateFunc.eMax :
            
            if isinstance(oResult, (int,  float)) :
                if (self.m_vValue[iIndex] == None) or (oResult > self.m_vValue[iIndex]) :
                    self.m_vValue[iIndex] = oResult


    def GetValue(self, iIndex : int) :
        
        oResult = None

        if self.m_eAggrFunc == enum_AggregateFunc.eCount :
            oResult = self.m_viQntRow[iIndex]

        elif self.m_eAggrFunc in (enum_AggregateFunc.eSum, enum_AggregateFunc.eMin, enum_AggregateFunc.eMax) :
            oResult = self.m_vValue[iIndex]

        elif self.m_eAggrFunc == enum_AggregateFunc.eAvg :
            oResult = self.m_vValue[iIndex] / self.m_viQntRow[iIndex]
       
        return oResult
        
    