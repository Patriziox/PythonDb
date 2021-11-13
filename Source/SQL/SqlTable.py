import pickle
from typing import Tuple
from typing_extensions import Literal

import SqlGlobals as Glob

from FileSystem import *
from SqlGeneric import SqlGeneric as Gen
from SqlError import SqlError, enum_Error
from SQL.SqlCheck import SqlCheck
from SQL.SqlColumn import SqlColumn
from SQL.SqlForeignKey import SqlForeignKey
from SQL.SqlRecord import SqlRecord
from SQL.SqlRecordHead import *
from SQL.SqlTableHead import SqlTableHead
from SQL.SqlColKeys import *
from SQL.SqlUnique import *
from SQL.SqlExpression import NULL


class SqlTable:
	
    THIS_NAME = 0
    THIS_QNT_COLS = 1
    THIS_QNT_PRIMARY_KEY = 2
    THIS_QNT_FOREIGN_KEY = 3
    THIS_QNT_CHECK = 4
    THIS_QNT_DEFAULT = 5
    THIS_INDEX = 6
    THIS_TEMPORARY = 7

    st_tAttributeToken = (
		('PRIMARY KEY', '_PrimaryKeyHnd'),
        ('FOREIGN KEY', '_ForeignKeyHnd'),
        ('CHECK', '_CheckHnd')
    )


    def __init__(self, sRoot : str = '', bTemporary = False):
       
        self.Kill()
        self.m_sRoot = sRoot
        self.m_bTemporary = bTemporary
        self.m_fTarget = None
        self.m_oHead = None
        self.m_oUnique = None
    
    def __eq__(self, item):
        return False if (item == None) else (self.m_sName == item.m_sName)

    def __ne__(self, item):
        return not self.__eq__(item)
    
    def __lt__(self, item):
        return True if (item == None) else (self.m_sName < item.m_sName)

    def __gt__(self, item):
        return True if (item == None) else (self.m_sName > item.m_sName)

    def __hash__(self):
        return hash(self.m_sName)

    def GetName(self):
        return self.m_sName

    def GetQCols(self):
        return len(self.m_voColumns)
	
    def Kill(self) :
       
        self.m_iIndex = -1
        self.m_sName = ""
        self.m_voColumns = []
        self.m_vPrimaryKey = []
        self.m_vForeignKey = []
        self.m_vCheck = []
        self.m_bTemporary = False
        self.m_oError = SqlError()
        self.m_vDefault = []

    def GetError(self) -> SqlError:
        return self.m_oError
        
    def Drop(self)->bool:
        
        sPath = f"{self.m_sRoot}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_DEF_EXT}"
        FileSystem.RemoveFile(sPath)

        sPath = f"{self.m_sRoot}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_HEAD_EXT}"
        FileSystem.RemoveFile(sPath)

        sPath = f"{self.m_sRoot}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_DAT_EXT}"
        FileSystem.RemoveFile(sPath)
      
    def _PrimaryKeyHnd(self, sQuery : str) -> bool:
        
        if(self.m_vPrimaryKey != []):
            self.m_oError = SqlError(enum_Error.eNotPossible, 'Primary-key already declared')
            return False

        sNop1, vsColNames, sNop2 = Gen.FunctionParser('NOP' + sQuery)
		
        if((sNop1 == None) or (sNop2 != '') or (vsColNames == [])) :
            
            self.m_oError = SqlError(enum_Error.eSyntax, 'Primary-Key Attribute')
            return False
		
        for sColName in vsColNames:
            
            bFound = False

            for oColumn in self.m_voColumns:
                if(sColName == oColumn.m_sName):
                    
                    oColumn.SetPrimaryKey()
                    
                    self.m_vPrimaryKey.append(oColumn)
                   
                    bFound = True
                    break

            if(bFound == False):

                self.m_oError = SqlError(enum_Error.eSyntax, 'Unknow Column on Primary-Key Attribute')
                return False
		
		#end for
		
        return True

    def _ForeignKeyHnd(self, sQuery : str) -> bool:
        
        oForeignKey = SqlForeignKey()

        if(oForeignKey.Parse(sQuery) == False):
            return False

        self.m_vForeignKey.append(oForeignKey)
       
        return True

    def _CheckHnd(self, sQuery : str) -> bool:

        oCheck = SqlCheck()

        if(oCheck.Parse(sQuery, self.m_oHead.GetKeys()) == False):
            return False

        self.m_vCheck.append(oCheck)
       
        return True

    def Open(self) -> bool:

        try:

            if self.m_fTarget :
                if not self.m_fTarget.closed :
                    return True

            sFullPath = f"{self.m_sRoot}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_DAT_EXT}"

            if not path.isfile(sFullPath) :
                return False
        
            self.m_fTarget = open(sFullPath, "rb")

            return True

        except :

            return False
        
    def Close(self):

        if self.m_fTarget :
            self.m_fTarget.close()
        

    def _addColumn(self, sColPars : str, oColsKeys : SqlColKeys, iColIndex : int) -> Tuple[SqlColumn, bool, bool]:
                 
        oColumn = SqlColumn(self)
        
        if not oColumn.Parse(sColPars, oColsKeys) :
            self.m_oError = oColumn.Error().GetNote(f"Column #{iColIndex} : {oColumn.Error().GetNote()}")
            return None, False, False

        for oCol in self.m_voColumns :
            if oCol == oColumn :
                self.m_oError = SqlError(enum_Error.eNotPossible, f'Column {oColumn.GetName()} already declared in this table')
                return None, False, False
        
        if oColumn.IsPrimaryKey() :
            if self.m_vPrimaryKey :
                self.m_oError = SqlError(enum_Error.eNotPossible, 'Primary-key already declared')
                return None, False, False
            
            self.m_vPrimaryKey.append(oColumn)

        bAutoInc = oColumn.IsAutoInc()
        
        bNull = False
        
        if oColumn.IsNull() :
            
            iQntNullCol = 0
            
            for oCol in self.m_voColumns :
                if oCol.IsNull() :
                    iQntNullCol += 1
                    
            oColumn.InitNullMask(iQntNullCol)
            bNull = True

        self.m_voColumns.append(oColumn)

        return oColumn, bAutoInc, bNull
    
    def Parse(self, sQuery: str) -> bool:
     
        oColsKeys = SqlColKeys()

        sName, vArgs, sCoda = Gen.FunctionParser(sQuery)

        if(sName == None) or (sCoda != "") :
			
            self.m_oError = SqlError(enum_Error.eSyntax)
            return False
            
        if(Gen.IsName(sName) == False):
            self.m_oError = SqlError(enum_Error.eSyntax, 'Bad Table name')
            return False
			
        self.m_sName = sName.strip()

        self.m_oHead = SqlTableHead(self.m_sName)
        self.m_oHead.SetKeys(oColsKeys)
        
        iQntAutoIncCol = 0
        iQntNullCol = 0

        iCol = 0
        for sArg in vArgs:
            # eventuali impostazioni di tabella
            
            oHandler = None

            for vAttribute in self.st_tAttributeToken:
				
                sBody = Gen.IsToken(sArg, vAttribute[0])
				
                if(sBody != None):
                                        
                    oHandler = getattr(self, vAttribute[1])

                    if(oHandler(sBody) == False):
                        
                        self.Kill()
                        
                        return False

                    break
            #   
            if(oHandler != None):
                continue
				
            # oColumn = SqlColumn(self)
            
            # if(oColumn.Parse(sArg, oColsKeys) == False):
                
            #     self.Kill()
                
            #     self.m_oError = oColumn.Error().GetNote(f"Column #{iCol} : {oColumn.Error().GetNote()}")
                
            #     return False

            # if(oColumn.IsPrimaryKey() == True):
            #     if(self.m_vPrimaryKey != []):
                    
            #         self.m_oError = SqlError(enum_Error.eNotPossible, 'Primary-key already declared')
            #         return False
                
            #     self.m_vPrimaryKey.append(oColumn)

            # if(oColumn.IsAutoInc() == True):
            #     iQntAutoIncCol += 1

            # if(oColumn.IsNull() == True):
                
            #     oColumn.InitNullMask(iQntNullCol)
            #     iQntNullCol += 1

            # self.m_voColumns.append(oColumn)

            oColumn, bAutoInc, bNull = self._addColumn(sArg, oColsKeys, iCol)
            
            if not oColumn :
                self.Kill()
                return False
            
            if bAutoInc :
                iQntAutoIncCol += 1
                
            if bNull :
                iQntNullCol += 1
                
            iCol += 1

        #end for
             
        # self.m_oError = SqlError(iQntAutoIncCol)
        
        self.m_oHead.SetAutoIncValue([1] * iQntAutoIncCol)
        
        if(iQntNullCol > 0):
            self.m_oHead.InitNull(iQntNullCol)
 
        return True

    def Save(self) -> bool:
		
        try :

            objHash = []

            objHeader = (
                self.m_sName, 
                len(self.m_voColumns),
                len(self.m_vPrimaryKey),
                len(self.m_vForeignKey),
                len(self.m_vCheck),
                len(self.m_vDefault),
                self.m_iIndex,
                self.m_bTemporary

            )

            FileSystem.CreateFolder(self.m_sRoot)

            with FileSystem() as oFileTable :

                if oFileTable.Open(f"{self.m_sRoot}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_DEF_EXT}", "wb") :
                    return False

                pickle.dump(objHeader, oFileTable.Raw())

                objHash.append(objHeader)

                viAutoIncValue = []

                for col in self.m_voColumns:
                    
                    colHash = col.Save(oFileTable.Raw())
                    objHash.append(colHash)

                    if(col.IsAutoInc() == True):
                        viAutoIncValue.append(col.GetAutoInc())
                
                pkeys = []

                for col in self.m_vPrimaryKey :
                    pkeys.append(col.m_sName)

                tpkeys = tuple(pkeys)

                pickle.dump(tpkeys, oFileTable.Raw())

                objHash.append(Gen.Crc(tpkeys))

                for item in self.m_vForeignKey:
                    
                    itHash = item.Save(oFileTable.Raw())
                    objHash.append(itHash)

                for oCheck in self.m_vCheck:
                    
                    itHash = oCheck.Save(oFileTable.Raw())
                    objHash.append(itHash)

                pickle.dump(Gen.Crc(objHash), oFileTable.Raw())
            
            ####################### 
            # table head
            ####################### 
            
            self.m_oHead.SetIndex(self.m_iIndex)
            self.m_oHead.SetAutoIncValue(viAutoIncValue)
            self.m_oHead.Save(self.m_sRoot)

            return True

        except :

            self.Kill()

            return False
	
    def Load(self, sName : str) -> bool :

        objHash = []
        self.m_oHead = SqlTableHead(sName)
		
        if(self.m_oHead.Load(self.m_sRoot) == False):
            return False

        viAutoIncValue = self.m_oHead.GetAutoIncValues()

        fSource = open(f"{self.m_sRoot}\\{Glob.TABLE_PREFIX}{sName}{Glob.TABLE_DEF_EXT}", "rb")

        objHeader = pickle.load(fSource)

        objHash.append(objHeader)

        self.m_sName = objHeader[SqlTable.THIS_NAME]
        self.m_iIndex = objHeader[SqlTable.THIS_INDEX]
        self.m_bTemporary = objHeader[SqlTable.THIS_TEMPORARY]

        iAutoIncIndex = 0

        for hh in range(objHeader[SqlTable.THIS_QNT_COLS]):
            
            col = SqlColumn()
            
            self.m_voColumns.append(col)

            col.SetIndex(hh)

            objHash.append(col.Load(fSource, self.m_oHead.GetKeys()))

            if(col.IsAutoInc() == True):
                
                col.SetAutoInc(viAutoIncValue[iAutoIncIndex])
                iAutoIncIndex += 1
        
        #primary key
        pkeys = pickle.load(fSource)

        objHash.append(Gen.Crc(pkeys))

        for colName in pkeys:

            for col in self.m_voColumns:
                if col.m_sName == colName:
                    self.m_vPrimaryKey.append(col)
                    break

        #foreignkey

        for hh in range(objHeader[SqlTable.THIS_QNT_FOREIGN_KEY]):
            
            obj = SqlForeignKey()
            
            self.m_vForeignKey.append(obj)

            objHash.append(obj.Load(fSource))

        # check

        for hh in range(objHeader[SqlTable.THIS_QNT_CHECK]):
            
            oCheck = SqlCheck()
            
            self.m_vCheck.append(oCheck)

            objHash.append(oCheck.Load(fSource, self.m_oHead.GetKeys()))
          

        Hash1 = pickle.load(fSource)

        fSource.close()

        Hash2 = Gen.Crc(objHash)

        if Hash1 == Hash2 :
            return True

        self.Kill()
        return False
      		
    def InsertInto(self, vsColumns : list, vValues : list) -> bool:

        try :
            
            if(vsColumns == []):
                vsColumns = self.GetColsName()

            if(len(vsColumns) != len(vValues)):
                self.m_oError = SqlError(enum_Error.eSyntax, f"expected {len(vsColumns)} parameters")
                return False

            vCols, vVals = self._GetDefault()

            for iIndex, sColName in enumerate(vsColumns):

                for iCol, oCol in enumerate(self.m_voColumns):
                    if(oCol.GetName() == sColName):
                        vVals[iCol] = vValues[iIndex]
                        break
                else :
                
                    self.m_oError = SqlError(enum_Error.eUnknownItem, f"Column #{iIndex} {sColName} : 'Unknown Column")
                    return False

            for iIndex, oCol in enumerate(self.m_voColumns):
            
                if((vVals[iIndex] == NULL) and (oCol.IsNull() == False)):

                    self.m_oError = SqlError(enum_Error.eConstraitVioletion, f"Column #{iIndex} {oCol.GetName()} : 'Not Null' violation")
                    return False

                if(oCol.IsUnique() == True and oCol.IsAutoInc() == False): 
                    if (self.m_oUnique == None):
			
                        self.m_oUnique = SqlUnique(self.m_sName)
                        self.m_oUnique.Load()

                    if(self.m_oUnique.Check(oCol.GetName(), vVals[iIndex]) == True):

                        self.m_oError = SqlError(enum_Error.eConstraitVioletion, f"Column #{iIndex} {oCol.GetName()} : 'Unique Value' violation")
                        return False
                
                oCheck = oCol.GetCheck()

                if(oCheck != None):
                    if(oCheck.Evalute(vVals) == False):
                        self.m_oError = SqlError(enum_Error.eConstraitVioletion, f"Column #{iIndex} {oCol.GetName()} : 'Check for Value' violation")
                        return False
              
                
            if self.m_vCheck != [] :
                for oCheck in self.m_vCheck :
                    if(oCheck.Evalute(vVals) == False):

                        self.m_oError = SqlError(enum_Error.eConstraitVioletion, 'Check For Values Violation')
                        return False

            self.m_fTarget = open(f"{self.m_sRoot}\\{Glob.TABLE_PREFIX}{self.m_sName}{Glob.TABLE_DAT_EXT}", "ab")
            
            oRecord = SqlRecord(self)

            bFeedBack = False
            
            if oRecord.Parse(vCols, vVals) :
                bFeedBack = oRecord.Save()
            
            viAutoIncValue = []
                  
            if(bFeedBack == True):
                
                bSaveUnique = False

                for iIndex, oCol in enumerate(self.m_voColumns):
                    
                    iAutoIncValue = oCol.IncAutoInc()

                    if(iAutoIncValue != None):
                        viAutoIncValue.append(iAutoIncValue)
                    
                    if(oCol.IsUnique() == True and oCol.IsAutoInc() == False): 
                        self.m_oUnique.Add(oCol.GetName(), vVals[iIndex])
                        bSaveUnique = True

                if(viAutoIncValue != []):
                    self.m_oHead.SetAutoIncValue(viAutoIncValue)
                    self.m_oHead.Save(self.m_sRoot)

                if( bSaveUnique == True):
                    self.m_oUnique.Save()
            
            self.m_fTarget.close()
		
            return bFeedBack
        
        except :
    
            self.m_fTarget.close()

            return False

    ####################
    ## GETTER/ SETTER ##
    ####################

    def Columns(self) -> list :
        return self.m_voColumns

    def Count(self) -> int :
        return len(self.m_voColumns)

    def Column(self, sName) -> SqlColumn:
        for oCol in self.m_voColumns:
            if(oCol.GetName() == sName):
                return oCol

        return None
	
    def GetTarget(self) :
	    return self.m_fTarget
    
    def GetIndex(self) -> int:
        return self.m_iIndex

    def SetIndex(self, iIndex):
        
        if self.m_iIndex != iIndex :
       
            self.m_iIndex = iIndex
            self.m_oHead.SetIndex(iIndex)

    def GetColsName(self, bFullName : bool = False) -> list:
        
        vNames = []
        
        if(bFullName == True):
            for oCol in self.m_voColumns:
                vNames.append(f"{self.m_sName}.{oCol.GetName()}")
        else:
            for oCol in self.m_voColumns:
                vNames.append(oCol.GetName())

        return vNames

    def _GetDefault(self)-> list and list:
        
        vsName = []
        vValues = []
        
        for oCol in self.m_voColumns:
            vsName.append(oCol.GetName())
            vValues.append(oCol.GetDefault())

        return vsName, vValues
   
    def AddColumn(self, sQuery : str) -> bool :
        
        oColsKeys = self.m_oHead.GetKeys()
        
        iColIndex = len(self.m_voColumns)
        
        oColumn, bAutoInc, bNull = self._addColumn(sQuery, oColsKeys, iColIndex)
            
        if not oColumn :
            return False
            
        if bAutoInc :
            iQntAutoIncCol = len(self.m_oHead.GetAutoIncValues()) + 1
            self.m_oHead.SetAutoIncValue([1] * iQntAutoIncCol)
            
        if bNull :
            iQntNullCol = self.m_oHead.GetQntNullCols() + 1
            self.m_oHead.InitNull(iQntNullCol)
        
        return True
    
    def DropColumn(self, sColName : str) -> bool :
                
        oColumn : SqlColumn
        
        oColumn = None
        voColumns = []
        
        for oCol in self.m_voColumns :
            if oCol == oColumn :
                oColumn = oCol
            else :
                voColumns.append(oCol)
        
        if not oColumn :    
            self.m_oError = SqlError(enum_Error.eColumnUndefined, f"Not found Column named '{sColName}'")
            return False
                        
        if oColumn.IsAutoInc() :
            iQntAutoIncCol = len(self.m_oHead.GetAutoIncValues()) - 1
            self.m_oHead.SetAutoIncValue([1] * iQntAutoIncCol)
            
        if oColumn.IsNull() :
            iQntNullCol = self.m_oHead.GetQntNullCols() - 1
            self.m_oHead.InitNull(iQntNullCol)
        
        self.m_oHead.GetKeys().Delete(oColumn.GetName())
                    
        self.m_voColumns = voColumns
        
        return True            
   
#end class        
        