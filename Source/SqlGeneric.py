import hashlib
import random
import binascii
from typing import Generic, Tuple

class SqlGeneric:

    st_tKeyword = (
        'AS',
        'BY',
        'FROM',
        'GROUP',
        'INNER',
        'JOIN',
        'SELECT',
        'WHERE',
        # ##########################
        # da completare
    )

    st_tsOper = (
        '+', '-', '*', '/', '%',
        '=', '<', '>',
        '&', '|',
    )

    @staticmethod
    def Normalize(oQuery : str | bytes) -> str:
        
        # l'interno dei doppi apici resta immutato
        # i caratteri sono maiuscoli
        # i caratteri \t \n \r sono sostituiti da ' '
        # sequenze di ' ' sono sostituite da un singolo ' '
        # dopo una lettere, un numero, ) c'e' uno ' '
        # prima di ( c'e' uno ' '

        sQuery = oQuery if isinstance(oQuery, str) else oQuery.decode("utf-8") if isinstance(oQuery, bytes) else ''

        if(len(sQuery) == 0):
            return ""
        
        bOnApice = False
        cLastCh = ' '

        sOut = ""

        for ch in sQuery:

            if (bOnApice == True):
                if((ch == '\"') and (cLastCh != '\\')):
                    bOnApice = False

            elif (ch == '\"'):
                bOnApice = True
            
            elif ch in (' ', '\t', '\n', '\r') :
                if (cLastCh == ' ') or not( (cLastCh == '\"') or ((cLastCh >= 'A') and (cLastCh <= 'Z')) or ((cLastCh >= '0')and (cLastCh <= '9')) ):
                    continue #evito sequenze di blank

                ch = ' ' 
           
            elif ch in (')', ']', '}') :
                if(cLastCh == ' ') :
                    sOut = sOut[0:-1]
				
                sOut = f"{sOut} {ch}"
                ch = ' '
			
            elif ch in ('+', '-', '*', '/', '(') :
                if(cLastCh == ' ') :
                    sOut += ch
                else:
                    
                    sOut = f"{sOut} {ch}"

                ch = ' '

            elif (ch == '=') :
                if cLastCh not in ('<', '>', ' ') :
                    sOut = f"{sOut} ="
                else:
                    sOut = f"{sOut}="
                
                ch = ' '
            elif (ch == '>') :
                if cLastCh not in ('<', ' ') :
                    sOut = f"{sOut} >"
                else:
                    sOut = f"{sOut}>"

                cLastCh = ch
                continue

            elif ch in ('&', '|') :
                if ch == cLastCh :
                    sOut = f"{sOut}{ch}"
                    ch = ' '
                elif cLastCh != ' ' :
                    sOut = f"{sOut} "
            
            elif ((ch >= 'a') and (ch <= 'z')):
                ch = ch.upper()

                if(cLastCh in SqlGeneric.st_tsOper):
                    sOut = f"{sOut} "

            elif ((ch >= 'A') and (ch <= 'Z')):

                if(cLastCh in SqlGeneric.st_tsOper):
                    sOut = f"{sOut} "

            elif (ch >= '0') and (ch <= '9'):
                if(cLastCh in SqlGeneric.st_tsOper):
                    sOut = f"{sOut} "

            cLastCh = ch
            sOut += ch
        
        #end for

        return sOut[0:-1] if(sOut[-1] == ' ') else sOut

    @staticmethod
    def IsName(sName : str) -> bool :
        
        if not isinstance(sName, str) :
            return False

        if len(sName) == 0 :
            return False
	
        cFirstCh = sName[0]	

        if cFirstCh != '_' :
            if((cFirstCh < 'A') or (cFirstCh > 'Z')):
                return False
        
        sNome = sName[1:]
	
        for ch in sNome :
            if(ch != '_'):
                if((ch < 'A') or (ch > 'Z')):
                    if((ch < '0') or (ch > '9')):
                        return False
        
        return (sName not in SqlGeneric.st_tKeyword)
    #end def

    @staticmethod
    def IsColumn(sColName : str) -> bool :
                
        if not isinstance(sColName, str) :
            return False

        if not len(sColName) :
            return False
	
        vsItems = sColName.split('.')

        iQntItems = len(vsItems)

        if iQntItems > 2 :
            return False
        
        if not SqlGeneric.IsName(vsItems[0]) :
            return False

        if iQntItems == 1 :
            return True

        return SqlGeneric.IsName(vsItems[1])
        
    #end def

    @staticmethod
    def FunctionParser(sSource : str) -> tuple:
      
        if not isinstance(sSource, str) :
            return None, None, sSource

        vArgs = []

        iCntTonde = 0
        bOnApice = False
        cLastCh = ' '
        
        iInit = sSource.find('(')

        if iInit == -1 :
            return None, None, sSource

        sFuncName = sSource[0:iInit].strip()

        if SqlGeneric.IsName(sFuncName) == False :
            return None, None, sSource

        sSource = sSource[iInit:]

        iInit = 1
        iEnd = 0

        bEndOfFunc = False

        for ch in sSource:
            if  bOnApice == True :
                if((ch == '\"') and (cLastCh != '\\')):
                    bOnApice = False

            elif ch == '\"' :
                bOnApice = True

            elif ch == ',' :
                
                if(iCntTonde == 1):
                  vArgs.append(sSource[iInit: iEnd].strip())
                  iInit = iEnd + 1

            elif ch == '(' :
                iCntTonde += 1
            
            elif ch == ')' :
                iCntTonde -= 1

                if(iCntTonde == 0):
                    vArgs.append(sSource[iInit: iEnd].strip())
                    bEndOfFunc = True
                    break
        
            cLastCh = ch
            iEnd += 1
        
        if bEndOfFunc == False :
            return None, None, sSource

        return sFuncName, vArgs, sSource[iEnd + 1:].strip()

    @staticmethod
    def RemoveTonde(sSource) -> Tuple[str, bool]:

        sSource = sSource.strip()
        
        lung = len(sSource)

        if((lung == 0) or (sSource[0] != '(') or (sSource[-1] != ')')) :
            return sSource, False

        bOnApice = False
        cLastCh = ' '

        hh = 0
        iCntTonde = 0
        iIndexLastCh = lung - 1

        for ch in sSource :
            
            if bOnApice == True :
                if((ch == '\"') and (cLastCh != '\\')):
                    bOnApice = False

            elif ch == '\"' :
                bOnApice = True
            
            elif ch == '(' :
                iCntTonde += 1
            
            elif ch == ')' :
                iCntTonde -= 1

                if iCntTonde == 0 :

                    if hh == iIndexLastCh :
                        sResult, bResult =  SqlGeneric.RemoveTonde(sSource[1:-1])

                        return sResult, True

                    break

            cLastCh = ch
            hh += 1
        
		#end for

        return sSource, False

    @staticmethod
    def IsToken(sTarget, *vsToken : str) -> str or None:
        
        for sToken in vsToken :

            if not isinstance(sToken, str) :
                return None

            lung = len(sToken)

            if(sTarget[0:lung] == sToken):
                return sTarget[lung:]
        
        return None

    @staticmethod
    def Split(sSource : str, sSplitStr : str) -> Tuple[str, str, bool] :

        if not (isinstance(sSource, str) and isinstance(sSplitStr, str)) :
            return sSource, '', False

        bOnApice = False
        cLastCh = ' '
        
        lung = len(sSplitStr)

        if lung == 0 :
	        return sSource, '', False

        cFirstCh = sSplitStr[0]
        hh = 0
        iCntTonde = 0

        for ch in sSource:
            
            if (bOnApice == True):
                if((ch == '\"') and (cLastCh != '\\')):
                    bOnApice = False

            elif (ch == '\"'):
                bOnApice = True
            
            elif (ch == '(') :
                iCntTonde += 1
            
            elif (ch == ')'):
                iCntTonde -= 1

            elif (ch == cFirstCh) and (iCntTonde == 0):
                if(sSource[hh: hh + lung] == sSplitStr):
                    return sSource[0:hh], sSource[hh + lung :], True
                    
            cLastCh = ch
            hh += 1

        return sSource, '', False
        #end for

    @staticmethod
    def ToInteger(sValue):

        if(type(sValue) is int):
            return True

        if(type(sValue) is not str):
            return False

        bOnHead = True

        for ch in sValue:

            if((ch == '+') or (ch == '-')):
                if(bOnHead == False):
                    return False			

            else:
                
                bOnHead = False
                        
                if((ch < '0') or (ch > '9')):
                    return False
            
        return True	

    @staticmethod
    def Crc(oTupla : tuple) -> int:

        oHash = hashlib.md5()
		
        for field in oTupla :
			
            sField = str(field)
            oHash.update(sField.encode())

        return oHash.digest()
    
    @staticmethod
    def CrcS(sSource : str) -> int:
        return binascii.crc32(sSource.encode('utf-8'))

    @staticmethod
    def CrcBytes(bySource : bytes) -> int:
        return binascii.crc32(bySource)
    
    @staticmethod
    def SplitFileName(sFullPath : str) -> tuple :
        
        iExtPos = sFullPath.rfind('.')
        iNamePos = sFullPath.rfind('\\')

        if(iNamePos == -1) :
            if (iExtPos == -1):
                return "", sFullPath, ""
            
            return "", sFullPath[0: iExtPos], sFullPath[iExtPos:]

        if (iExtPos == -1):
                return sFullPath[0:iNamePos], sFullPath[iNamePos + 1 :], ""
            
        return sFullPath[0:iNamePos], sFullPath[iNamePos + 1 : iExtPos], sFullPath[iExtPos:]

    @staticmethod
    def Cut(sSource : str, sSep : str) -> list:

        vsResult = []
        vsCuts = sSource.split(sSep)

        for sCut in vsCuts :
            vsResult.append(sCut.strip())

        return vsResult
    
    @staticmethod
    def GetHandler(obj : object, sQuery : str, vTokenList : list) -> tuple:
        
        iPos = 1000
        sHead = None
        sFoot = None
        oPar = None
        iIndex = None

        for iIndexTmp, oToken in enumerate(vTokenList) :
            
            sHeadTmp, sFootTmp, bResult = SqlGeneric.Split(sQuery, oToken[0])

            if bResult :
                
                iPosTmp = len(sHeadTmp)

                if iPosTmp < iPos :
                    sHead = sHeadTmp
                    sFoot = sFootTmp
                    oPar = oToken[1]
                    iPos = iPosTmp
                    iIndex = iIndexTmp

                    if iPos == 0 :
                        break

        if iIndex is not None:
            
            fuHandler = getattr(obj, oPar)
            return iIndex, fuHandler, sHead, sFoot
                
        return None, None, sQuery, None


    @staticmethod
    def SetPassword(sPassword : str, sPwdFoot : str = None) -> bytes :
        
        oHash = hashlib.sha256()
        oHash.update(sPassword.encode('utf-8'))

        if sPwdFoot is None :
            random.seed()
            sPwdFoot = str(random.getrandbits(32))

        oHash.update(sPwdFoot.encode('utf-8'))
            
        return oHash.hexdigest(), sPwdFoot
    
    @staticmethod
    def Inside(item, vList : list) -> int :
        for iIndex, it in enumerate(vList) :
            if it == item :
                return iIndex
        
        return -1

    @staticmethod
    def Extends(vList1 : list, vList2 : list) -> int :

        vTarget = vList1.copy()

        for item in vList2 :
            if item not in vList1 :
                vTarget.append(item)

        return vTarget

    @staticmethod
    def BeginsWith(sSource : str, sHead : str) -> str :

        sSrc = sSource.lstrip()

        lung = len(sHead)

        if sSrc[0 : lung] == sHead :
            return sSrc[lung :]
        
        return ""
    
    @staticmethod
    def GetString(sSource : str) -> str :
        
        if len(sSource) > 1 :
            if sSource[0] == '"' and sSource[-1] == '"' :
               return sSource[1 : -1]

        return sSource

    @staticmethod
    def CreatePermuta(vsSrc : list[str], sSep : str = ',') -> list[str] :
        
        iCntSrc = len(vsSrc)
        iTotItems = pow(2, iCntSrc)
        iStep = iTotItems 

        vTargetTmp = [''] * iTotItems
        viQntItems = [0] * iTotItems
        
        for hh in range(iCntSrc) :

            iIndex = 0
            iStep //= 2 

            while iIndex < iTotItems :
                for jj in range(iStep) :
                    vTargetTmp[iIndex] = f'{vTargetTmp[iIndex]}{sSep}{vsSrc[hh]}' if vTargetTmp[iIndex] else vsSrc[hh]
                    viQntItems[iIndex] += 1
                    iIndex += 1
                
                iIndex += iStep
        
        vTarget = []

        for iSize in range(iCntSrc, -1, -1) :
            for jj in range(iTotItems) :
                if viQntItems[jj] == iSize :
                    vTarget.append(vTargetTmp[jj])
        
        return vTarget
            