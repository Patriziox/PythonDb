import pickle
import socket
from struct import *
from SqlGlobals import enum_TcpToken



class TcpFrame :

    def __init__(self, eToken : enum_TcpToken, oData = None, *vPars) :
        self.m_eToken = eToken
        self.m_vPars = vPars
        self.m_oData = oData

    def __eq__(self, eTcpToken: enum_TcpToken) -> bool:
        return (self.m_eToken == eTcpToken)

    def __ne__(self, eTcpToken: enum_TcpToken) -> bool:
        return (self.m_eToken != eTcpToken)

    def GetToken(self) :
        return self.m_eToken

    def GetData(self) :
        return self.m_oData

    def GetTuples(self) :
        return pickle.loads(self.m_oData)

class TcpSocket:
    
    def __init__(self, oSocket : socket) -> None:
        self.m_oSocket = oSocket
   
    def Send(self, eAction : enum_TcpToken, sData : str = '', *vPars):

        oTcpFrame = TcpFrame(eAction, sData, vPars)
        
        self.m_oSocket.send(pickle.dumps(oTcpFrame))

    def Recv(self, iTotByte : int = 0) -> TcpFrame:
                         
        return pickle.loads(self.m_oSocket.recv(iTotByte))
