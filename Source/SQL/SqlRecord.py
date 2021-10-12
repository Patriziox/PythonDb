from copy import deepcopy
from struct import *

import SqlGlobals as Glob
from SQL.SqlTable import *
from SQL.SqlColumn import *
from SQL.SqlRecordHead import *
from SQL.SqlExpression import NULL

class SqlRecord:

    NULL_BYTES = pack('I', 0)

    def __init__(self, oTable ) :
        
        self.Kill()
        self.m_oTable = oTable
        self.m_oHead.InitNullMask(oTable.m_oHead.GetQntNullCols())
        

    def Kill(self):
        self.m_oHead = SqlRecordHead()
        self.m_oTable = None
        self.m_vFields = []
           
    def GetField(self, iIndex):
        return self.m_vFields[iIndex]

    def GetFieldSet(self) -> list:
        return self.m_vFields

    def Clone(self) -> list:
        vFields = deepcopy(self.m_vFields)
        return vFields

    def Size(self) -> int:
        return len(self.m_vFields)

    def Save(self):
        
        try :
			
            fTarget = self.m_oTable.GetTarget()
        
            sformat = ""
           
            oBody = bytearray()
                
            iIndex = 0

            for col in self.m_oTable.Columns():

                it = self.m_vFields[iIndex] 

                if it == NULL :
                    
                    bin = SqlRecord.NULL_BYTES

                else :
                    
                    eType = col.GetType()

                    if eType == enum_DataType.eVarchar :
                                    
                        lung = len(it)
                        sformat = f'I {lung}s'
                        
                        bin = pack(sformat, lung, it.encode())

                    elif eType == enum_DataType.eInt : 
                        bin = pack('I', it)

                oBody.extend(bin)
                iIndex += 1
            
            self.m_oHead.SetSize(len(oBody))
            
            self.m_oHead.Save(fTarget)
            
            fTarget.write(oBody)

            return True
        
        except :

            return False

    def Load(self) -> bool:
        
        fSource = self.m_oTable.GetTarget()
        
        self.m_vFields = []

        iQntByte = self.m_oHead.Load(fSource)

        if(iQntByte == 0):
            return False

        oBuff = fSource.read(iQntByte)

        # iNullColsIndex = 0

        for oCol in self.m_oTable.Columns():
            
            eType = oCol.GetType()

            if(eType == enum_DataType.eVarchar)  :
                              
                lung = unpack('I', oBuff[:4])[0]

                ilast = lung + 4

                item = unpack(f'{lung}s', oBuff[4: ilast])[0]

                if(oCol.IsNull() == True):
                    if(oCol.CheckForNull(self.m_oHead.GetNullMask()) == True):
                        item = NULL

                self.m_vFields.append(item)

                oBuff = oBuff[ilast:]                

            elif(eType == enum_DataType.eInt) : 
                
                item = unpack('I', oBuff[:4])[0]

                if(oCol.IsNull() == True):
                    if(oCol.CheckForNull(self.m_oHead.GetNullMask()) == True):
                        item = NULL

                self.m_vFields.append(item)

                oBuff = oBuff[4:] 

        return True

    def Parse(self, vCols : list, vVals : list):
        
        for oCol in self.m_oTable.Columns():
            self.m_vFields.append(oCol.GetDefault())
               
        for sCol in vCols:
 
            oCol = self.m_oTable.Column(sCol)

            if oCol == None:
                return False
            
            iIndex = oCol.GetIndex()

            if vVals[iIndex] == 'NULL' :
                
                oCol.SetNullMask(self.m_oHead.GetNullMask())

                oValue = NULL
           
            else :
                
                oValue = oCol.Encode(vVals[iIndex])
            
                if oValue == None :
                    return False

            self.m_vFields[iIndex] = oValue
            
        return True
