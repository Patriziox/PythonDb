import os
import sys
import json

# sys.path.append('c:\\Users\\Patrizio\\myProject\\Python\\SqlEngine\\Source')
sys.path.append('d:\\myProject\\Python\\PythonDb\\Source')

import unittest
import pickle

from SQL.SqlDataBase import *
from SqlGlobals import enum_TcpToken


@unique
class enum_TestMode(Enum):
    eCheck = 'check'
    eCheckSorted = 'checksorted'
    

class TestSqlSelect(unittest.TestCase) :
    
    def _parseTupla(self, oTupla) :

        vTupla = []

        for field in oTupla :

            if isinstance(field, bytes) :
                vTupla.append(field.decode('UTF-8'))
            
            else :
                vTupla.append(field)

        return vTupla


    def setUp(self) :
       
        self.m_oDataBase = SqlDataBase()
            
        # sFullPath = f'{Glob.DB_FOLDER}\\{TestSqlSelect.sDBName}\\{TestSqlSelect.sDBName}{Glob.DB_EXT}'

        # self.m_oDataBase.Load(sFullPath, 'anag') 
    
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


    def test_Select(self) :
       
        tQuery = (
            ('001', 'Test_001'),
            ('001', 'Test_002'),
            ('001', 'Test_003'),
            ('001', 'Test_004'),
            ('001', 'Test_006'),
            ('001', 'Test_007'),
            ('001', 'Test_008'),
            ('001', 'Test_009'),
            ('001', 'Test_011'),
            ('001', 'Test_012'),
            ('001', 'Test_013'),
            ('GroupBy', 'Test_001'),
            ('GroupBy', 'Test_002'),
            ('GroupBy', 'Test_003'),
            ('GroupBy', 'Test_004'),
            ('Union', 'Test_001'),

        )

        for tItem in tQuery :

            print('')
            print(f'{tItem[1]}...')

            sFullPath = f'{os.path.dirname(__file__)}\\TestResult\\{tItem[0]}\\{tItem[1]}.json'

            jsonTest = None

            with open(sFullPath, 'rt') as fSource:
                jsonTest = json.loads(fSource.read())


            sFullPath = f'{Glob.DB_FOLDER}\\{jsonTest["DBName"]}\\{jsonTest["DBName"]}{Glob.DB_EXT}'

            self.m_oDataBase.Load(sFullPath, jsonTest['DBPwd']) 


            eResult, oResult = self.m_oDataBase.Query(jsonTest['query'])

            sMsg = ''

            if eResult == enum_TcpToken.eReplyFailure :
                
                oResult = pickle.loads(oResult)

                sMsg = f'{str(oResult[0])} : {oResult[1]}'
            
            elif eResult == enum_TcpToken.eReplyTuple :
                
                jsonTuple = jsonTest['tuples']

                oSqlTuple = pickle.loads(oResult)
               
                self.assertEqual(oSqlTuple.GetLabels(), jsonTest['caption'])
                             
                sCheckMode = jsonTest['checkMode']

                print(oSqlTuple.GetLabels())
                
                if sCheckMode == enum_TestMode.eCheck.value :
										
                    self.assertEqual(len(oSqlTuple.GetTuples()), len(jsonTuple))
					     
                    for oTupla in oSqlTuple.GetTuples() :
                   
                        print(oTupla)
                        
                        vTupla = self._parseTupla(oTupla)

                        self.assertIn(vTupla, jsonTuple)

                elif sCheckMode == enum_TestMode.eCheckSorted.value :
					
                    iIndex = 0
                    vTuple = oSqlTuple.GetTuples()
                    iQntTuple = len(vTuple)
                    
                    for jTupla in jsonTuple :
                        
                        if isinstance(jTupla, dict) :
                            
                            vjTuple = jTupla['data']
                        
                        else :
                            
                            vjTuple = [jTupla]
                        
                        rStep = range(len(vjTuple))
                        
                        for hh in rStep :
                            
                            self.assertLess(iIndex, iQntTuple)
                            
                            print(vTuple[iIndex])
                            
                            vTupla = self._parseTupla(vTuple[iIndex])
                            
                            for ii in rStep :
                            
                                if vTupla == vjTuple[ii] :
                                    
                                    vjTuple[ii] = None
                                    break
                            else :
                                
                                self.fail(f'{vTupla} not found')
                            
                            iIndex += 1
                            					
                    self.assertEqual(iIndex, iQntTuple)
					
                print('Tuple: #', len(oSqlTuple.GetTuples()))

            self.assertEqual(eResult, enum_TcpToken.eReplyTuple, '\n' + tItem[0] + '\n' + sMsg)

if __name__ == '__main__' :
    unittest.main()