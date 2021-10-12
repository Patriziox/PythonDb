import socket
from SqlGlobals import enum_TcpToken

from TCP_IP.TcpCommands import *
from TCP_IP.TcpSocket import *


class SqlShell:

    THIS_TOT_BYTE = 4096

    def __init__(self) :
        self.m_oSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def Connect(self, oHost = '127.0.0.1', iPort : int = 9999):
        self.m_oSocket.connect((oHost, iPort))
  
    def Run(self):

        oTcpSocket = TcpSocket(self.m_oSocket)

        if __debug__ :

            tsRequest = (
                        'login "admin" "admin"',
                        # 'pwd "admin" "abc"',
                        # 'createdb "anagrafe" "anag"',
                        'opendb "anagrafe" "anag"',
                        
                        # 'create table Individuo(Uid int primary key auto_increment, Nome varchar(50) not null, Eta int, uidCitta int, Peso int)',

                        # 'insert into Individuo Values(0, "Mario", 40, 1, 85)',
                        # 'insert into Individuo Values(0, "Anna", 17, 3, 42)',
                        # 'insert into Individuo Values(0, "Luca", 75, 2, 59)',
                        # 'insert into Individuo Values(0, "Paolo", 36, 1, 102)',
                        # 'insert into Individuo Values(0, "Laura", 12, null, 78)',
                        # 'insert into Individuo Values(0, "Carlo", 80, 4, 78)',
                        # 'insert into Individuo Values(0, "Giulio", 99, 4, 54)',
                        # 'insert into Individuo Values(0, "Elisa", 62, 3, 90)',
                        # 'insert into Individuo Values(0, "Camila", 11, 1, 77)',
                        # 'insert into Individuo Values(0, "Ettore", 18, 2, 45)',
                        # 'insert into Individuo Values(0, "Anna", 47, 3, 117)',
                        # 'insert into Individuo Values(0, "Giulio", 9, 4, 35)',
                        # 'insert into Individuo Values(0, "Luca", 80, 2, null)',
                        

                        # 'create table Citta(uidCitta int primary key auto_increment, Nome varchar(50) not null, QntAbitanti int)',
                        # 'insert into citta Values(0, "Tokio", 100)',
                        # 'insert into citta Values(0, "Parigi", 80)',
                        # 'insert into citta Values(0, "Roma", 60)',
                        # 'insert into citta Values(0, "Londra", 85)',
                        # 'insert into citta Values(0, "Madrid", 55)',

                        # 'create table Citta(uidCitta int primary key auto_increment, Nome varchar(50) not null, QntAbitanti int)',

                        # 'select * from individuo ',

                        # 'select individuo.name, citta.name from individuo ,citta ',
                        # 'select * from individuo ,citta where func(QntAbitanti) > 100 ',
                        # 'select name, eta from individuo where eta between 30 and 50',
                        # 'select uidFamiglia, count(uidFamiglia) from individuo',
                        # 'select uidFamiglia, count(uidFamiglia) from individuo group by uidFamiglia',
                        # 'select uidFamiglia, avg(eta), count(uidFamiglia) from individuo group by uidFamiglia',
                        # 'select individuo.name from individuo minus select citta.name from citta',
                        # 'select individuo.name, citta.name from individuo, citta where exists select citta.name  from citta where citta=uidCitta &&  QntAbitanti > 60',
                        # 'select citta.name from citta where QntAbitanti > (select Avg(QntAbitanti) from citta) order by citta.name',
                        # 'select Individuo.name from Individuo where citta in (select uidCitta from citta where QntAbitanti <80)',
                        # 'select individuo.Nome, QntAbitanti as popolazione from individuo join citta on individuo.uidCitta=citta.uidCitta',
                        # 'select individuo.Nome, citta.Nome as city, QntAbitanti as popolazione from individuo join citta on individuo.uidCitta=citta.uidCitta',
                        # 'select individuo.Nome, case when Eta < 18 then "NO" when Eta < 21 then "Deputati" else "Senatori" end "VOTO"  from individuo ',
                        # 'select individuo.Nome from individuo where Peso < case (when eta < 40 then 50 else 80) ',
                        # 'select citta.uidCitta, citta.nome, avg(peso) as avgPeso from individuo, citta where individuo.uidCitta=citta.uidCitta group by individuo.uidCitta having avgPeso >= 66',
                        # 'select uidCitta as citta, individuo.Nome from individuo order by uidCitta, Nome'
                        # 'select uid, nome, peso * 1000 as grammi from individuo where uidcitta < 3 order by grammi asc',
                        # 'select * from individuo, citta where INDIVIDUO.uidcitta = citta.uidCitta',
                        # 'select Individuo.uid, Individuo.nome, citta.nome from individuo join citta on INDIVIDUO.uidcitta = citta.uidCitta'
                        # 'select uidCitta as citta, individuo.Nome from individuo order by uidCitta desc, Nome asc'
                        # 'select Individuo.uid, Individuo.nome, citta.nome from individuo left join citta on INDIVIDUO.uidcitta = citta.uidCitta order by citta.uidcitta,INDIVIDUo.nome'
                        # 'select Individuo.uid, Individuo.nome, citta.nome, case(when eta > 60 then \"OLD\" when eta > 18 then \"BIG\" else \"BABY\" end as stato) from individuo join citta on INDIVIDUO.uidcitta = citta.uidCitta order by citta.uidcitta,INDIVIDUo.nome'
            )

            for sRequest in tsRequest :
            
                oTcpSocket.Send(enum_TcpToken.eReqQuery, sRequest)
                
                oTcpFrame = oTcpSocket.Recv(SqlShell.THIS_TOT_BYTE)

                print('reply : ', oTcpFrame.GetToken())
                
                if oTcpFrame == enum_TcpToken.eReplyTuple :
                    oSqlTuple = oTcpFrame.GetTuples()

                    print(oSqlTuple.GetLabels())

                    
                    for oTupla in oSqlTuple.GetTuples() :
                        print(oTupla)

                    print('Tuple: #', len(oSqlTuple.GetTuples()))

                else :

                    print('Data : ', oTcpFrame.GetData())
        else :

            self.m_oSocket.settimeout(3)
        
        # end debug

        while(True):

            sRequest = input('> > >')

            # if sRequest.upper() ==  TCP_CMMD_EXIT :
                
                # print("That's all folks")

                # return True

            oTcpSocket.Send(enum_TcpToken.eReqQuery, sRequest)
            
            try :
                
                oTcpFrame = oTcpSocket.Recv(SqlShell.THIS_TOT_BYTE)

                print('reply : ', oTcpFrame.GetAction())
                
                print('Data : ', oTcpFrame.GetData())
            
            except socket.timeout as oErrInfo: 

                print("Time out")