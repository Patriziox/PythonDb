#import tkinter
#tkinter._test()

from SQL.SqlDataBase import *
from TCP_IP.TcpServer import *
from Shell.SqlShell import *

# oExp = SqlExp()

# oExp.Parse(Gen.Normalize('Peso < case when eta < 40 then 50 else 80'))

# l = oExp.Evalute(None)

# n = Gen.Normalize('x&&abc')



oServer = TcpServer()
oServer.start()

oShell = SqlShell()
oShell.Connect()
oShell.Run()
