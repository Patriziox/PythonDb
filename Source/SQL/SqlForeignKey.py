import pickle
from SqlGeneric import SqlGeneric as Gen

from enum import Enum, auto

class enum_ForeignKeyReaction(Enum):
    eNone = 0
    eRestrict = 1,
    eCascade = 2,
    eSetNull = 3,
    eSetDefault = 4,
   
   
class SqlForeignKey:

    st_vForeignReaction = [
		['RESTRICT ', enum_ForeignKeyReaction.eRestrict],
		['CASCADE ', enum_ForeignKeyReaction.eCascade],
        ['SET NULL ', enum_ForeignKeyReaction.eSetNull],
		['NO ACTION ', enum_ForeignKeyReaction.eNone],
        ['SET DEFAULT ', enum_ForeignKeyReaction.eSetDefault]
	]

    def __init__(self):
        self.m_voColKey = []
        self.m_sExternTable = ""
        self.m_vsExternCol = []
        self.m_eOnDelete = enum_ForeignKeyReaction.eNone
        self.m_eOnUpdate = enum_ForeignKeyReaction.eNone

    def __hash__(self):

        objToHash = (
            tuple(self.m_voColKey),
            self.m_sExternTable,
            tuple(self.m_vsExternCol),
            str(self.m_eOnDelete),
            str(self.m_eOnUpdate)
        )

        return hash(objToHash)

    def _OnDeleteHnd(self, sQuery):
        
        if(sQuery[-1] != ' '):
            sQuery = sQuery + ' '
        
        for vReaction in self.st_vForeignReaction:
            
            sReaction = Gen.IsToken(sQuery, vReaction[0])
        
            if (sReaction != None):
                self._m_eOnDelete = vReaction[1]

                return sReaction
        
        return None

    def _OnUpdateHnd(self, sQuery):
           
        if(sQuery[-1] != ' '):
            sQuery = sQuery + ' '
            
        for vReaction in self.st_vForeignReaction:
            
            sReaction = Gen.IsToken(sQuery, vReaction[0])
        
            if (sReaction != None):
                self.m_eOnUpdate = vReaction[1]

                return sReaction
        
        return None
    
    def Parse(self, sQuery):

        sNop1, vsColNames, sQuery = Gen.FunctionParser('NOP' + sQuery)

        if(vsColNames == None):
            return None
        
        self.m_voColKey = vsColNames

        sQuery = Gen.IsToken(sQuery, 'REFERENCES ')

        if(sQuery == None):
            return None
        
        sExtTable, vsColNames, sQuery = Gen.FunctionParser(sQuery)

        if(Gen.IsName(sExtTable) == False):
            return None
        
        if(len(vsColNames) == 0):
            return None

        self.m_sExternTable = sExtTable

        self.m_vsExternCol = vsColNames
                   
        sBody = Gen.IsToken(sQuery, 'ON DELETE ')

        if(sBody != None):
            sQuery = self._OnDeleteHnd(sBody)

            bOnDelete = True

        else :
            sBody = Gen.IsToken(sQuery, 'ON UPDATE ')

            if(sBody != None):
                sQuery = self._OnUpdateHnd(sBody)

                bOnDelete = False
            else:
                return None
        
        if(bOnDelete == True):

            sBody = Gen.IsToken(sQuery, 'ON UPDATE ')

            if(sBody != None):
                sQuery = self._OnUpdateHnd(sBody)

        else :

            sBody = Gen.IsToken(sQuery, 'ON DELETE ')

            if(sBody != None):
                sQuery = self._OnDeleteHnd(sBody)

        return sQuery

    def Serialize(self):

        return tuple((
            tuple(self.m_voColKey),
            self.m_sExternTable,
            tuple(self.m_vsExternCol),
            self.m_eOnDelete,
            self.m_eOnUpdate
        ))


    def Save(self, fTarget):

        objTupla = self.Serialize()

        pickle.dump(objTupla, fTarget)

        return hash(objTupla)

    def Load(self, fSource):

        objTupla = pickle.load(fSource)

        self.m_voColKey = objTupla[0]
        self.m_sExternTable = objTupla[1]
        self.m_vsExternCol = objTupla[2]
        self.m_eOnDelete = enum_ForeignKeyReaction(objTupla[3])
        self.m_eOnUpdate = enum_ForeignKeyReaction(objTupla[4])

        return hash(objTupla)

        