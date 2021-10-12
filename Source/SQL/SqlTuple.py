import pickle

from SQL.SqlSchema import *

class SqlTupleModel :
    def __init__(self, vsLabels : list) :
        self.m_vsLabels = vsLabels 
        self.m_vvTuple = [] 

    def __getitem__(self, iIndex : int):
        return self.m_vvTuple[iIndex]

    def __setitem__(self, iIndex : int, oValue):
        self.m_vvTuple[iIndex] = oValue

    def Add(self, oTupla : list) :
        self.m_vvTuple.append(oTupla)

    def GetLabels(self) -> list:
        return self.m_vsLabels

    def GetTuples(self) -> list:
        return self.m_vvTuple
        
    def SetTuples(self, vvTuple):
        self.m_vvTuple = vvTuple

class SqlTuple:

    def __init__(self, vsLabels : list, oSchema : SqlSchema) :

        self.m_oSchema = oSchema 
        self.m_oData = SqlTupleModel(vsLabels)

    def __iter__(self):
        return iter(self.m_oData.GetTuples())

    def __getitem__(self, iIndex : int):
        return self.m_oData[iIndex]

    def __setitem__(self, iIndex : int, oValue):
        self.m_oData[iIndex] = oValue

    def Add(self, oTupla : list) :
        self.m_oData.Add(oTupla)

    def GetLabels(self) -> list:
        return self.m_oData.GetLabels()

    def GetTuples(self) -> list:
        return self.m_oData.GetTuples()
        
    def SetTuples(self, vvTuple) :
        return self.m_oData.SetTuples(vvTuple)

    def GetSchema(self) -> SqlSchema :
        return self.m_oSchema

    def Serialize(self) -> bytes:
        return pickle.dumps(self.m_oData)

    def Deserialize(self, byData : bytes) :
        pickle.loads(self.m_oData, byData)