import pickle
# from enum import Enum, auto

from SqlGeneric import SqlGeneric as Gen
from SqlGlobals import enum_DataType
from SQL.SqlCheck import *

from SqlError import *

class SqlColumn:
	
	st_vDefaultDataValues = [
		None,
		0,
		'',
		0, #False
		0, #Time
	]

	st_vDataTypeToken = [
		['INT', enum_DataType.eInt],
		['VARCHAR', enum_DataType.eVarchar],
		['BOOL', enum_DataType.eBool],
		['DATE', enum_DataType.eDate],
		['TIME', enum_DataType.eTime]
	]
	
	st_vPropertyToken = [
		['NOT NULL ', '_NotNullHnd'],
		['NULL ', '_NullHnd'],
		['UNIQUE ', '_UniqueHnd'],
		['PRIMARY KEY ', '_PrimaryKeyHnd'],
		['AUTO_INCREMENT ', '_AutoIncrementHnd'],
		['VISIBLE ', '_VisibleHnd'],
		['INVISIBLE ', '_InvisibleHnd'],
		['DEFAULT ', '_DefaultLiteralHnd'],
		['DEFAULT (', '_DefaultExpHnd'],
		['COMMENT ', '_CommentHnd'],
		['CHECK (', '_CheckHnd']
	]
		
	def __init__(self, oTable = None):
		self.Kill()
		
		self.m_iAutoIncValue = 1
		self.m_oTable = oTable
		
	def Error(self):
		return self.m_oError
	
	def __eq__(self, oCol : 'SqlColumn') -> bool :
		
		if isinstance(oCol, SqlColumn) :
			return oCol.GetName() == self.m_sName 

		return False

	def Kill(self):
		self.m_iIndex = None
		self.m_sName = ''
		self.m_sAlias = ""
		self.m_sComment = ""
		self.m_eType = enum_DataType.eNone
		self.m_iSize = 0
		self.m_bNull = True
		self.m_bPrimaryKey = False
		self.m_bAutoIncrement = False
		self.m_bVisible = True
		self.m_bUnique = False
		self.m_sDefault = "" #valore estratto dalla query
		self.m_oDefaultValue = None #effettivo valore di default in funzione del tipo dato
		self.m_oCheck = None
		self.m_oError = SqlError()

		self.m_oColKeys = None
		
		# self.m_bbNullMask = b'\0'
		# self.m_bbNotNullMask = b'\0'
		self.m_iNullMask = 0
		self.m_iNotNullMask = 0
		self.m_iNullIndex = 0
	
	def _NotNullHnd(self, sQuery : str):
		self.m_bNull = False
		return sQuery

	def _NullHnd(self, sQuery : str):
		self.m_bNull = True
		return sQuery

	def _UniqueHnd(self, sQuery : str):
		self.m_bUnique = True
		
		return sQuery

	def _PrimaryKeyHnd(self, sQuery : str):
		
		self.SetPrimaryKey()

		return sQuery

	def _AutoIncrementHnd(self, sQuery : str):
		self.m_bAutoIncrement = True
		return sQuery

	def _VisibleHnd(self, sQuery : str):
		self.m_bVisible = True
		return sQuery
	
	def _InvisibleHnd(self, sQuery : str):
		self.m_bVisible = False
		return sQuery
	
	def _DefaultLiteralHnd(self, sQuery : str):
		
		iPos = sQuery.find(' ')

		self.m_sDefault = sQuery[0:iPos]

		return sQuery[iPos + 1:]
	
	def _DefaultExpHnd(self, sQuery : str):
		
		iPos = sQuery.find(')')
		
		self.m_sDefault = sQuery[0:iPos]

		return sQuery[iPos + 1:]

	def _CommentHnd(self, sQuery : str):

		iPos = sQuery.find(' ')

		self.m_sComment = sQuery[0:iPos]

		return sQuery[iPos + 1:]

	def _CheckHnd(self, sQuery : str):

		iPos = sQuery.find(')')

		sCheck = sQuery[0:iPos].strip()
		
		self.m_oCheck = SqlCheck()
		
		oTableKeys = SqlTableKeys()
		oTableKeys.Update(self.m_oTable.GetName(), self.m_oColKeys)

		self.m_oCheck.Parse(sCheck, oTableKeys)

		return sQuery[iPos + 1:]

	def _SetDefaultValue(self):
		
		if(self.m_sDefault == ''):
			self.m_oDefaultValue = self.st_vDefaultDataValues[self.m_eType.value]

		oValue = self.Encode(self.m_sDefault)

		if(oValue == None):
			return False

		self.m_oDefaultValue = oValue
			
		return True

	def Parse(self, sQuery : str, oColsKeys : SqlColKeys) -> bool:

		sName, sBody, bResult = Gen.Split(sQuery, ' ')

		if(Gen.IsName(sName) == False) :
			
			self.m_oError = SqlError(enum_Error.eSyntax, 'Bad Column name')
			return False
		
		self.m_iIndex = self.m_oTable.Count()

		self.SetName(sName)
				
		bOnError = True
		
		sType, vsArgs, sBody = Gen.FunctionParser(sBody)

        # individuo il tipo dato
		if(sType == None): #il tipo dato non presenta una size
						
			sType, sBody, bResult = Gen.Split(sBody, ' ')
						
			iSize = 0

		else:
			
			if(len(vsArgs) != 1) or (Gen.ToInteger(vsArgs[0]) == False):
				
				self.m_oError = SqlError(enum_Error.eSyntax, 'Bad Datatype param')
				return False
			
			iSize = int(vsArgs[0])

		if(sType != None):
			for sDataType in self.st_vDataTypeToken:
				if(sType == sDataType[0]):
					
					self.m_eType = sDataType[1]
					self.m_iSize = iSize
					
					bOnError = False
					break

		if(bOnError == True):
			
			self.m_oError = SqlError(enum_Error.eSyntax, 'Unknown Datatype')
			return False
		
		#--

		oColsKeys.Update(sName, self.m_oTable.GetIndex(), self.m_iIndex, self.m_eType)

		self.m_oColKeys = oColsKeys		

		while(sBody != "") :
			
			Handler = None
									
			for Prop in self.st_vPropertyToken:
				
				sQuery = Gen.IsToken(sBody + ' ' , Prop[0])
				
				if(sQuery != None):
					
					Handler = getattr(self, Prop[1])
					
					sBody = Handler(sQuery).strip()

					break
			
			if(Handler == None):
				
				self.m_oError = SqlError(enum_Error.eSyntax, 'Unknown Column property')
				return False
		
		if(self._SetDefaultValue() == False):
			return False

		self.m_oError = SqlError()
				
		return True

	def Serialize(self):
		
		return tuple((

			self.m_sName,
			self.m_sComment,
			self.m_eType,
			self.m_iSize,
			self.m_bNull,
			self.m_bPrimaryKey,
			self.m_bAutoIncrement,
			self.m_bVisible,
			self.m_bUnique,
			self.m_sDefault,
			'' if self.m_oCheck == None else self.m_oCheck.GetExpr(),
			self.m_iNullIndex,
			# self.m_bbNullMask
			self.m_iNullMask
		))
	
	def Unserialize(self, objTuple, oColKeys : SqlColKeys):
		
		self.SetName(objTuple[0])
		self.m_sComment = objTuple[1]
		self.m_eType = enum_DataType(objTuple[2])
		self.m_iSize = objTuple[3]
		self.m_bNull = objTuple[4]
		self.m_bPrimaryKey = objTuple[5]
		self.m_bAutoIncrement = objTuple[6]
		self.m_bVisible = objTuple[7]
		self.m_bUnique = objTuple[8]
		self.m_sDefault = objTuple[9]
		
		if objTuple[10] == '':
			self.m_oCheck = None
		else:
			self.m_oCheck = SqlCheck()
			self.m_oCheck.Parse(objTuple[10], oColKeys)

		self.m_iNullIndex = objTuple[11]
		# self.m_bbNullMask = objTuple[12]
		# self.m_bbNotNullMask = ~ self.m_bbNullMask[0]

		self.m_iNullMask = objTuple[12]
		self.m_iNotNullMask = ~self.m_iNullMask

		self.m_oDefaultValue = self.Encode(self.m_sDefault)
			
	def Save(self, fTarget):
		
		objTuple = self.Serialize()
		
		pickle.dump(objTuple, fTarget)

		return Gen.Crc(objTuple)
		
	def Load(self, fSource, oColKeys : SqlColKeys) -> bytes :

		objData = pickle.load(fSource)
		
		self.Unserialize(objData, oColKeys)
		
		objTuple = self.Serialize()

		return Gen.Crc(objTuple)

	def SetName(self, sName):
		self.m_sName = sName
		
		if(self.m_sAlias == ''):
			self.m_sAlias = sName

	def GetDefault(self):
		return self.m_oDefaultValue

	def GetIndex(self):
		return self.m_iIndex
	
	def SetIndex(self, iIndex):
		self.m_iIndex = iIndex

	def GetName(self):
		return self.m_sName

	def GetAlias(self):
		return self.m_sAlias

	def IsAutoInc(self):
		return self.m_bAutoIncrement

	def GetAutoInc(self):
		return self.m_iAutoIncValue
	
	def IncAutoInc(self):
		if(self.m_bAutoIncrement == False):
			return None
		
		self.m_iAutoIncValue = self.m_iAutoIncValue + 1
		return self.m_iAutoIncValue
	
	def SetAutoInc(self, iValue):
		self.m_iAutoIncValue = iValue

	def GetType(self) -> enum_DataType:
		return self.m_eType

	def GetCheck(self) -> SqlCheck: 
		return self.m_oCheck

	def IsUnique(self) -> bool:
		return self.m_bUnique
	
	def IsPrimaryKey(self) -> bool:
		return self.m_bPrimaryKey

	def SetPrimaryKey(self):
		self.m_bPrimaryKey = True
		self.m_bNull = False
		self.m_bUnique = True

	def IsNull(self):
		return self.m_bNull

	def InitNullMask(self, bit : int):
		self.m_iNullIndex = bit // 8
		# bbMask = 1 << (bit % 8)
		# self.m_bbNullMask = bbMask.to_bytes(1, 'little')
		self.m_iNullMask =  1 << (bit % 8)

	def SetNullMask(self, vbbNullMask : bytearray):
		# vbbNullMask[self.m_iNullIndex] |= self.m_bbNullMask
		vbbNullMask[self.m_iNullIndex] |= self.m_iNullMask

	def ResetNullMask(self, vbbNullMask : bytearray):
		# vbbNullMask[self.m_iNullIndex] &= ~self.m_bbNotNullMask
		vbbNullMask[self.m_iNullIndex] &= ~self.m_iNotNullMask
	
	def CheckForNull(self, vbbNullMask ) -> bool:
		
		# return ((vbbNullMask[self.m_iNullIndex] & self.m_bbNullMask) != 0)
		return ((vbbNullMask[self.m_iNullIndex] & self.m_iNullMask) != 0)
	
	def Encode(self, sValue):

		try:
			
			if(self.m_bAutoIncrement == True):
				return self.m_iAutoIncValue

			if(self.m_eType == enum_DataType.eVarchar):
				return sValue.replace('"', '')
				
			if(self.m_eType == enum_DataType.eInt):
				return 0 if sValue == '' else int(sValue)

			if(self.m_eType == enum_DataType.eBool):
				if(sValue == 'FALSE'):
					return 0

				if(sValue == 'TRUE'):
					return 1

				raise Exception()
				

		except :
			
			self.m_oError = SqlError(enum_Error.eBadValue, '')
			return None

	def Check(self, oValue):
		
		Value = self.Encode(oValue)

		if(Value == None):
			return None
		
		if(self.m_oCheck == None):
			return Value
		
