import socket
from threading import Thread
from TCP_IP.TcpCliHandler import *
from SqlError import *

class TcpServer(Thread):

    def __init__(self, iTotConnection : int = 5, sHost : str = '127.0.0.1', iPort : int = 9999) -> None:
        
        super().__init__()
        self.m_iTotConnection = iTotConnection
        self.m_sHost = sHost
        self.m_iPort = iPort
        self.m_oError = SqlError()

        self.Initialize()


    def Initialize(self):

        DbUser.Initialize()



    def run(self) -> None:
        
        try:
            self.m_oSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        except OSError as oErrInfo:
           
            self.m_oSocket = None
            self.m_oError = SqlError(enum_Error.eOffline, f'winError {oErrInfo.winerror} : {oErrInfo.strerror}')

        try:
            # self.m_oSocket.bind((socket.gethostname(), self.m_iPort))
            self.m_oSocket.bind((self.m_sHost, self.m_iPort))
            self.m_oSocket.listen(self.m_iTotConnection)
            
            while(True):
                
                oClientSocket, address = self.m_oSocket.accept()
                oClientHnd = TcpCliHandler(oClientSocket)
                oClientHnd.Run()

        except OSError as oErrInfo:
            
            self.m_oSocket.close()
            self.m_oSocket = None
            self.m_oError = SqlError(enum_Error.eOffline, f'winError {oErrInfo.winerror} : {oErrInfo.strerror}')
        

        