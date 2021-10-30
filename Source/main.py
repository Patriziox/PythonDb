#import tkinter
#tkinter._test()

from SQL.SqlDataBase import *
from TCP_IP.TcpServer import *
from Shell.SqlShell import *


oServer = TcpServer()
oServer.start()

oShell = SqlShell()
oShell.Connect()
oShell.Run()
