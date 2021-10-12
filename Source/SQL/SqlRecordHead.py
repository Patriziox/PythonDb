from io import FileIO
from struct import *

class SqlRecordHead:

    def __init__(self) -> None:
        self.m_iQntByte = 0
        self.m_bbFlag = bytearray(1)
        self.m_bbNullMask = None

    def SetSize(self, iQntByte : int)->None:
         self.m_iQntByte = iQntByte

    def InitNullMask(self, iQntNull : int):
        if(iQntNull > 0):
            self.m_bbNullMask = bytearray(1 + (iQntNull - 1) // 8)

    def GetNullMask(self) -> bytearray:
        return self.m_bbNullMask

    def SetDelete(self, bState : bool)->None:
        self.m_bbFlag = (self.m_bbFlag | 0x01) if (bState == True ) else (self.m_bbFlag & 0xFE)

    def IsDelete(self) -> bool:
        return ((self.m_bbFlag & 0x01) != 0)

    def Serialize(self) ->tuple:
		
        oTupla = tuple((
            self.m_iQntByte,
			self.m_bbFlag,
            self.m_bbNullMask
		))
	
        return oTupla
	
    def Unserialize(self, oTupla):
	    
        self.m_iQntByte = oTupla[0]
        self.m_bbFlag = oTupla[1]
        self.m_bbNullMask = oTupla[2]

    def Load(self, fSource) -> int :
        byData = fSource.read(6)

        if(byData == b''):
            return 0

        self.m_iQntByte, self.m_bbFlag, lung = unpack('H 1s H', byData[:6])

        byData = fSource.read(lung)
        self.m_bbNullMask = unpack(f'{lung}s', byData[: lung])[0]
        
        return self.m_iQntByte
    
    def Save(self, fTarget) :
        
        if self.m_bbNullMask :

            lung = len(self.m_bbNullMask)
               
            fTarget.write(pack(f'H 1s H {lung}s', self.m_iQntByte, self.m_bbFlag, lung, self.m_bbNullMask))
        else :
            
            fTarget.write(pack(f'H 1s H', self.m_iQntByte, self.m_bbFlag, 0))

        
    def Clone(self)  :
        
        oItem = SqlRecordHead()
        oItem.m_bbNullMask = bytearray(len(self.m_bbNullMask))

        return oItem