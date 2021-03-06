from enum import Enum, unique
from typing_extensions import Literal

from SQL.SqlRecord import *
from SQL.SqlColumn import *
from SQL.SqlTuple import *
from SQL.SqlTables import *
from SQL.SqlAggregate import *
from SQL.SqlSchema import *
from SQL.SqlCase import *
from SQL.SqlNull import *

from SqlError import *

@unique
class enum_JoinMode(Enum):
    eInner = 1
    eLeft = 2
    eRight = 3
    eFull = 4

@unique
class enum_SqlMultiSelectType(Enum):
    eUnion = 0
    eIntersect = 1
    eMinus = 2
     
class SqlSelect:
   
    THIS_GROUPBY_QNT_TUPLE = 0
    THIS_GROUPBY_RULE = 1

    st_tSelectClause = (
        (' WHERE EXISTS ', '_WhereExistsParser', None),
        (' WHERE ', '_WhereParser', None),
		(' GROUP BY CUBE ', '_GroupByCubeParser', None),
		(' GROUP BY GROUPING SETS ', '_GroupingSetsParser_1', None),
		(' GROUP BY ROLLUP ', '_RollupParser_1', None),
		(' GROUP BY ', '_GroupByParser_1', None),
		(' ORDER BY ', '_OrderByParser', None),
		(' HAVING ', '_HavingParser', None),
		(' LIMIT ', '_LimitParser', None),
		(' LEFT JOIN ', '_JoinParser', enum_JoinMode.eLeft),
		(' RIGHT JOIN ', '_JoinParser', enum_JoinMode.eRight),
		(' FULL JOIN ', '_JoinParser', enum_JoinMode.eFull),
		(' JOIN ', '_JoinParser', enum_JoinMode.eInner),
		(' UNION ', '_UnionParser', None),
		(' INTERSECT ', '_IntersectMinusParser', enum_SqlMultiSelectType.eIntersect),
		(' MINUS ', '_IntersectMinusParser', enum_SqlMultiSelectType.eMinus),
    )

    st_tSelectAggregate = (
        (' COUNT ', '_CountHnd'),
        (' SUM ', '_AggrGenericHnd'),
        (' AVG ', '_AggrGenericHnd'),
        (' MIN ', '_AggrGenericHnd'),
        (' MAX ', '_AggrGenericHnd'),
    )

    def __init__(self, oTables : SqlTables):
        self.m_oTables = oTables
        self.m_vTablesName = []
        self.m_voColumns = []
        self.m_oError = None
        
        self.m_oWhere = None
        self.m_oHaving = None
        self.m_vvGroupBy = []
        self.m_voRollUp = []
        self.m_vbSortingMode = []
        self.m_voOrderBy = []
        self.m_voAggregate = []
        self.m_voExpression = []
        self.m_voSelectedCols = []
        self.m_iLimit = None
        self.m_veJoinMode = []
        self.m_voJoinRule = []
        self.m_oSchema = None
        self.m_tMultiSelect = None
        self.m_tExists = []
        self.m_voCase = []
    
    def GetError(self) -> SqlError :
        return self.m_oError 

    def Parse(self, sQuery : str) -> SqlTuple:
        
        bDistinct = False
        
        sResult = Gen.IsToken(sQuery.lstrip(), 'DISTINCT ')
        
        if sResult :
			
            sQuery = sResult
            bDistinct = True
        
        sColumns, sFoot, bResult = Gen.Split(sQuery, ' FROM ')

        if not bResult :

            self.m_oError = SqlError(enum_Error.eClauseMissing, '"FROM" clause missing')
            return None

        iIndexHnd, fuHandler, sTables, sFoot = Gen.GetHandler(self, sFoot, SqlSelect.st_tSelectClause)
              
        self.m_vTablesName = Gen.Cut(sTables, ',')
        
        while fuHandler  :
                        
            vArgs = SqlSelect.st_tSelectClause[iIndexHnd][2]

            if vArgs is None :
                
                bResult, iIndexHndTmp, sFoot, fuHandler = fuHandler(sFoot)
            
            else :
                
                bResult, iIndexHndTmp, sFoot, fuHandler = fuHandler(sFoot, vArgs)
                           
            if not bResult :
                
                self.m_oError = SqlError(enum_Error.eClauseVioletion, f'Syntax error in "{SqlSelect.st_tSelectClause[iIndexHnd][0].strip()}" clause')
                return None

            iIndexHnd = iIndexHndTmp
            
        if not self.m_vTablesName :
            
            self.m_oError = SqlError(enum_Error.eTableMissing, 'Table list empty')
            return None

        if self.m_tExists :
            for vsTables in self.m_tExists :

                vsUnknownTables = self._checkForTables(vsTables)

                if vsUnknownTables :
                    
                    self.m_oError = SqlError(enum_Error.eTableUndefined, f'Not found any Table(s) named : "{"".join(vsUnknownTables)}"')
                    return None

            if not self._columnsManager(sColumns.split(',')) :
                return None

            self.m_vTablesName = Gen.Extends(self.m_vTablesName, self.m_tExists[1])

        else : 
            
            vsUnknownTables = self._checkForTables(self.m_vTablesName)

            if vsUnknownTables :
                
                self.m_oError = SqlError(enum_Error.eTableUndefined, f'Not found any Table(s) named : "{"".join(vsUnknownTables)}"')
                return None

        vsCols = self._SelectedColumnsParser(sColumns)
        
        if not vsCols :
            return None

        oSchema = SqlSchema()

        if not self._columnsManager(vsCols, oSchema) :
            return None
                
        oSchema.Extend(self.m_oSchema)
        self.m_oSchema = oSchema

        # if self.m_vvGroupBy :
            # self._GroupByParser_2()

        if self.m_voRollUp :
            self._RollupParser_2()

        # nel 'GROUPING BY SETS' le colonne devono essere dichiarate nella clausola 'SELECT'
        elif len(self.m_vvGroupBy) > 1 :

            if not self._GroupingSetsParser_2() :
                return None

            for iNop, oSchema in self.m_vvGroupBy :
                for oSchemaItem1 in oSchema :
                    
                    if oSchemaItem1.GetType() == enum_SqlSchemaType.eEmpty :
                        break

                    for oSchemaItem2 in self.m_voSelectedCols :
                        if oSchemaItem1 == oSchemaItem2 :
                            break

                    else :

                        return None

        elif self.m_vvGroupBy :
            if not self._GroupByParser_2() :
                return None

        if self.m_voRollUp :

            voRollUp = []

            for oSchemaRollup in self.m_voRollUp :
                for oSchema in self.m_oSchema :
                    if oSchemaRollup == oSchema :
                       voRollUp.append(oSchema) 
                       break
                else :
                    return None
            
            self.m_voRollUp = voRollUp

        for oExp in self.m_voExpression :
            if not oExp.Parse(None, self.m_oSchema, True) :
                return None

        if self.m_oHaving :
            if not self._HavingManager() :
                return None

        for oAggregate in self.m_voAggregate :
            if not oAggregate.Parse(self.m_oSchema) :
                return None

        if self.m_oWhere :
    
            oWhereClause = SqlExp()

            if not oWhereClause.Parse(self.m_oWhere, self.m_oSchema, True) :
                
                self.m_oError = SqlError(enum_Error.eClauseVioletion, f'Violation in "WHERE" clause')
                return None

            oNode : SqlExpNode # annotation

            for oNode in oWhereClause.GetNodes() : # nella clausola where non ?? ammessa una funzione di aggragazione
                if oNode.GetAggrType() != None :
                    self.m_oError = SqlError(enum_Error.eClauseVioletion, f'Impossible to define "Aggregate function" in "WHERE" clause')
                    return None
                
                if oNode.GetType() == enum_ExpType.eCase :
                    
                    oCase = self._CaseParser(oNode.GetText())
                    
                    if not oCase :
                        return None

                    oNode.SetItem(oCase)

            self.m_oWhere = oWhereClause
            
        oCase : SqlCase

        for oCase in self.m_voCase :
            if not oCase.SetValue(self.m_oSchema) :
                self.m_oError = oCase.GetError()
                return None
        
        if self.m_vvGroupBy :
            
            for iIndex, vGroupItem in enumerate(self.m_vvGroupBy) :
				
                vGroupRule = vGroupItem[SqlSelect.THIS_GROUPBY_RULE]

                vGroupBy = []
                
                for oGroupCol in vGroupRule :
                    
                    if oGroupCol.GetType() != enum_SqlSchemaType.eEmpty :

                        for oSchema in self.m_oSchema :
                            if oSchema == oGroupCol :
                                vGroupBy.append(oSchema)
                                break
                        else :

                            return None
				
                self.m_vvGroupBy[iIndex] = [0, vGroupBy]

        if self.m_voOrderBy :
            if not self._OrderByManager() :
                return None


        vvMatrixRecords = []
                     
        for sTableName in self.m_vTablesName :
            
            oTable = self.m_oTables.GetTable(sTableName)

            if not oTable :
                return None

            if not oTable.Open() :
                return None

            vSingleTabRecords = []
            
            while(True):

                oRecord = SqlRecord(oTable)

                if not oRecord.Load() :
                    break
                
                vSingleTabRecords.append(oRecord)

            vvMatrixRecords.append(vSingleTabRecords)
            
            oTable.Close()
        
        if self.m_voJoinRule :
            
            voJoinRule = []

            for oJounRule in self.m_voJoinRule :
            
                oJoinClause = SqlExp()

                oJoinClause.Parse(oJounRule, self.m_oSchema)

                voJoinRule.append(oJoinClause)
            
            self.m_voJoinRule = voJoinRule

        return self._createTuples(vvMatrixRecords, bDistinct)

    def _checkForTables(self, vsTablesName : list) -> list :

        vsNotFound = []

        for sTableName in vsTablesName :
            if not self.m_oTables.GetTable(sTableName) :
                vsNotFound.append(sTableName)
        
        return vsNotFound

    def _checkForColumns(self, oColumn : str) -> SqlSchemaItem :
        
        if not isinstance(oColumn, str) :
            return None

        sTable, sColumn, bResult = Gen.Split(oColumn, '.')
        
        if not bResult :
            sColumn = sTable
            sTable = None
     
        oSchema : SqlSchemaItem 

        if sTable :
            for oSchema in self.m_voSelectedCols :
                if oSchema.GetTableName() == sTable :
                    if oSchema.GetColName() == sColumn :
                        return oSchema

            if self.m_oSchema :
                for oSchema in self.m_oSchema :
                    if oSchema.GetTableName() == sTable :
                        if oSchema.GetColName() == sColumn :
                            return oSchema

            return None
        
        oThisSchema = None

        for oSchema in self.m_voSelectedCols :
            if oSchema.GetColName() == sTable or oSchema.GetAlias() == sColumn :
                if oThisSchema :
                    return None
                
                oThisSchema = oSchema

        if oThisSchema :
            return oThisSchema

        if self.m_oSchema :
            for oSchema in self.m_oSchema :
                if oSchema.GetColName() == sTable or oSchema.GetAlias() == sColumn :
                    if oThisSchema :
                        return None
                    
                    oThisSchema = oSchema

        return oThisSchema

    def _SelectedColumnsParser(self, sColsName : str) -> list :
       
        vsAllColNames = []
        vsExpCols = []
       
        if sColsName == '*' :

            for sTableName in self.m_vTablesName :
                
                oTable = self.m_oTables.GetTable(sTableName)

                if not oTable :
                    return None

                vsSingleTableColNames = oTable.GetColsName(bFullName = True)

                oCol : SqlColumn

                for oCol in oTable.Columns() :
                    
                    oSchemaItem = SqlSchemaItem(enum_SqlSchemaType.eColumn, oTable.GetIndex(), oCol.GetIndex(), None, oCol.GetType(), sTableName, oCol.GetName(), f'{sTableName}.{oCol.GetName()}')
                    
                    self.m_voSelectedCols.append(oSchemaItem)

                vsAllColNames.extend(vsSingleTableColNames)
        else:

            vColsName = sColsName.split(',')
                  
            for sColName in vColsName:
                
                sFoot = Gen.BeginsWith(sColName, 'CASE ') 

                if sFoot :
                    
                    if not self._CaseParser(sFoot) :
                        return None

                    for iIndex, oCase in enumerate(self.m_voCase) :
                        
                        oSchemaItem = SqlSchemaItem(enum_SqlSchemaType.eCase, enum_SqlSchemaType.eCase.value, iIndex, enum_SqlSchemaType.eCase.value, enum_DataType.eNone, None, oCase.GetAlias(), oCase.GetAlias())

                        self.m_voSelectedCols.append(oSchemaItem)
                        vsAllColNames.append(oSchemaItem)

                    continue

                sText, sAlias, bExistAlias = Gen.Split(sColName, ' AS ')
                
                iIndex, fuHandler, sHead, sFoot = Gen.GetHandler(self, ' ' + sText, SqlSelect.st_tSelectAggregate)
                
                if iIndex is None : # no aggregate function

                    sColName = sHead.strip()     

                    if Gen.IsColumn(sColName) :
						
                        vsAllColNames.append(sColName)
                        
                        oTable, oCol = self.m_oTables.GetColumn(sColName)

                        if not oTable :
                            return None
                        
                        oTable : SqlTable
                        oCol : SqlColumn

                        self.m_voSelectedCols.append(
                                                    SqlSchemaItem(enum_SqlSchemaType.eColumn, 
                                                            oTable.GetIndex() , 
                                                            oCol.GetIndex(), 
                                                            None, oCol.GetType(), 
                                                            oTable.GetName(), 
                                                            oCol.GetName(), 
                                                            sAlias if sAlias else sColName)
                                                    )
                    
                    else :
					    
                        sText, bNop = Gen.RemoveTonde(sText)

                        oSchemaItem = SqlSchemaItem(enum_SqlSchemaType.eExpression, -1, len(self.m_voExpression), enum_SqlSchemaType.eExpression.value, enum_DataType.eNone, None, sText, sAlias)

                        vsAllColNames.append(oSchemaItem)
                        
                        oSqlExp = SqlExp(sColName)

                        vCols = oSqlExp.GetColumns(sColName)
                        
                        self.m_voExpression.append(oSqlExp)
                        
                        vsExpCols.extend(vCols)
                        
                        self.m_voSelectedCols.append(oSchemaItem)

                else :

                    sNop, sArgs, sFoot = Gen.FunctionParser('NOP' + sFoot)
                    
                    oAggregate, vCols = fuHandler(sArgs[0], enum_AggregateFunc(iIndex))

                    if not oAggregate :
                        return None

                    vsExpCols.extend(vCols)

                    oAggregate.SetIndex(len(self.m_voAggregate))
                    
                    sColName = Gen.Normalize(sText)

                    oSchemaItem = SqlSchemaItem(enum_SqlSchemaType.eAggregate, enum_SqlSchemaType.eAggregate.value, len(self.m_voAggregate), enum_SqlSchemaType.eAggregate.value, enum_DataType.eNone, None, sColName, sAlias)

                    vsAllColNames.append(oSchemaItem)
                                        
                    if bExistAlias :
                        oAggregate.SetAlias(sAlias)
                    
                   
                    oAggregate.SetText(sColName)
                    
                    self.m_voAggregate.append(oAggregate)
                    self.m_voSelectedCols.append(oSchemaItem)

            for sTableName in self.m_vTablesName :
                
                oTable : SqlTable

                oTable = self.m_oTables.GetTable(sTableName)

                if not oTable :
                    return None
               
                vsSingleTableColNames = oTable.GetColsName(bFullName = True)
                
                for sColName in vsSingleTableColNames :
                    if sColName not in vsAllColNames :
                        vsAllColNames.append(sColName)

        vToAdd = []

        for col1 in vsExpCols :
            for col2 in vsAllColNames :
                
                if isinstance(col2, SqlSchemaItem) :
                    if col2.GetColName() == col1 :
                        break

                elif col2 == col1 :
                    break
            else :
               
                vToAdd.append(col1)

        vsAllColNames.extend(vToAdd)

        return vsAllColNames

    def _columnsManager(self, voCols : list, oSchema : SqlSchema = None) -> bool:

        for item in voCols:
                                  			
            if isinstance(item, SqlSchemaItem) :
                
                oSchema.Append(item)
                continue
                
            if isinstance(item, str) :

                col = item
                sAlias = ''

            else :

                col, sAlias = item
			
            sp1, sp2, bResult = Gen.Split(col, '.')

            oTable = None
            oColumn = None

            if sp2 == '' : # no table
                
                sColName = sp1
                                
                for sTableName in self.m_vTablesName : # verifico se esiste una sola colonna

                    oTableTmp = self.m_oTables.GetTable(sTableName)

                    if not oTableTmp :
                        return False

                    oColumnTmp = oTableTmp.Column(sColName)

                    if oColumnTmp :
                        if oColumn : # esistono due colonne con lo stesso nome 
                            
                            self.m_oError = SqlError(enum_Error.eAmbiguousReference, f'ambiguous reference to column named : "{oColumn.GetName()}"')
                            return False

                        oTable = oTableTmp
                        oColumn = oColumnTmp

            else :
                
                if not oTable or oTable.GetName() != sp1 :
                    oTable = self.m_oTables.GetTable(sp1)

                if not oTable :

                    self.m_oError = SqlError(enum_Error.eTableUndefined, f'Not found any Table named : "{sp1}"')
                    return False
				
                sColName = sp2
				
                oColumn = oTable.Column(sColName)
                                
            if not oColumn :
                
                self.m_oError = SqlError(enum_Error.eColumnUndefined, f'Not found any Column named : "{sp2 or sp1}"')
                return False
            
            sThisTableName = oTable.GetName()

            for iIndex, sTableName in enumerate(self.m_vTablesName) :
                if sTableName == sThisTableName :
                    break
            else :
                
                self.m_oError = SqlError(enum_Error.eColumnUndefined, f'Not found any Column named : "{oColumn.GetName()}"')
                return False

            if oSchema != None :
                if oColumn not in self.m_voColumns :

                    self.m_voColumns.append(oColumn)

                    oSchema.Append(SqlSchemaItem(enum_SqlSchemaType.eColumn, oTable.GetIndex(), oColumn.GetIndex(), iIndex, oColumn.GetType(), sThisTableName, sColName, sAlias))
        
        return True
    
    def _WhereParser(self, sQuery : str) :
        
        iIndexHnd, oHandler, sWhereExp, sFoot = Gen.GetHandler(self, sQuery, SqlSelect.st_tSelectClause)

        self.m_oWhere = sWhereExp
    
        return True, iIndexHnd, sFoot, oHandler

    def _WhereExistsParser(self, sQuery : str) :
                
        sNop, sFoot, bResult = Gen.Split(sQuery, ' FROM ')

        if not bResult :
            return False, None, None, None

        sTables, sFoot, bResult = Gen.Split(sFoot, ' WHERE ')

        if not bResult :
            return False, None, None, None

        vMainTables = self.m_vTablesName[:]
        vSubTables = Gen.Cut(sTables, ',')
        
        iIndexHnd, oHandler, sWhereExp, sFoot = Gen.GetHandler(self, sFoot, SqlSelect.st_tSelectClause)

        self.m_oWhere = sWhereExp

        self.m_tExists = tuple((vMainTables, vSubTables, ))
    
        return True, iIndexHnd, sFoot, oHandler

    def _LimitParser(self, sQuery : str) -> Tuple[Literal[True], int, str, str] | Tuple[Literal[False], None, None, None] :
        
        iIndexHnd, Handler, sLimit, sFoot = Gen.GetHandler(self, sQuery, SqlSelect.st_tSelectClause)

        if not sLimit.isnumeric() :
            return False, None, None, None
        
        self.m_iLimit = int(sLimit)
        
        return True, iIndexHnd, sFoot, Handler

    def _GroupByParser_1(self, sQuery : str) -> Tuple[Literal[True], int, str, str] | Tuple[Literal[False], None, None, None] :
        
        iIndexHnd, oHandler, sColums, sFoot = Gen.GetHandler(self, sQuery, SqlSelect.st_tSelectClause)

        sColums, bResult = Gen.RemoveTonde(sColums)
       
        self.m_vvGroupBy = Gen.Cut(sColums, ',')

        return True, iIndexHnd, sFoot, oHandler
  
    def _GroupByParser_2(self) -> bool :
       		
        vvGroupBy = []
        
        for sColName in self.m_vvGroupBy:
            
            oSchemaItem = self._checkForColumns(sColName)
                       
            if not oSchemaItem :
                return False

            vvGroupBy.append(oSchemaItem)

        self.m_vvGroupBy = [[0, vvGroupBy]]
      
        return True

    def _GroupByManager(self, vvFullTupleSet : list) -> list:

        vGroupBy = []

        vGroupByRule = self.m_vvGroupBy[0][SqlSelect.THIS_GROUPBY_RULE]

        vGroupFullTuple = []
        
        for oSingleFullTupla in vvFullTupleSet:
            
            vGroupingValues = []

            for oHook in vGroupByRule :
                
                iTabIndex = oHook.GetIndex()
                iColIndex = oHook.GetColumn()

                oValue = oSingleFullTupla[iTabIndex][iColIndex]

                if oValue == 'NULL' :
                    break

                vGroupingValues.append(oValue)
            
            else :

                for iGroup, oValue in enumerate(vGroupBy) :
                    if vGroupingValues == oValue :
                        iGroupIndex = iGroup
                        break
                else :
                    
                    iGroupIndex = len(vGroupBy)
                    vGroupBy.append(vGroupingValues)

                    vGroupFullTuple.append(oSingleFullTupla)

                if self.m_voAggregate :
                    for oAggrFunc in self.m_voAggregate :
                        oAggrFunc.Evalute(oSingleFullTupla, iGroupIndex) 

        self.m_vvGroupBy[0][SqlSelect.THIS_GROUPBY_QNT_TUPLE] = len(vGroupFullTuple)

        return vGroupFullTuple 

    def _RollupParser_1(self, sQuery : str) -> Tuple[Literal[True], int, str, str] | Tuple[Literal[False], None, None, None] :
            
        iIndexHnd, oHandler, sColums, sFoot = Gen.GetHandler(self, sQuery, SqlSelect.st_tSelectClause)

        vsColums, bResult = Gen.RemoveTonde(sColums)

        if not bResult :
            return False, None, None, None

        self.m_voRollUp = vsColums

        return True, iIndexHnd, sFoot, oHandler

    def _RollupParser_2(self) -> bool :
       		
        vvGroupBy = []
        vRollUp = []
        
        vsColumns = Gen.Cut(self.m_voRollUp, ',')

        for sColName in vsColumns:
            
            oSchemaItem = self._checkForColumns(sColName)
                       
            if not oSchemaItem :
                return False

            vvGroupBy.append(oSchemaItem)
            vRollUp.append(oSchemaItem)

        self.m_vvGroupBy = [[0, vvGroupBy]]
        self.m_voRollUp = vRollUp
      
        return True

    def _rollupAggregateUpdater(self, oTupla : tuple, iThisRollup : int, vValues : list, viQntRow : list) :
                        
        for iIndexAggr, oAggr in enumerate(self.m_voAggregate) :
            
            oAggr : SqlAggregate
            
            match oAggr.GetType() :
                
                case enum_AggregateFunc.eSum | enum_AggregateFunc.eAvg :
                    
                    vValues[iIndexAggr][iThisRollup] += oTupla[enum_SqlSchemaType.eRollUp.value][iIndexAggr][1]
                
        viQntRow[iThisRollup] += oTupla[enum_SqlSchemaType.eRollUp.value][iIndexAggr][0]          
                

    def _rollupAddSubtotalRow(self, oPivot : tuple, iThisRollup : int, iSize : int, vValues : list, viQntRow : list) -> tuple:
                
        oNewRow = deepcopy(oPivot)
                
        for iRollUp in range(iThisRollup, iSize) :

            oRollUp = self.m_voRollUp[iRollUp] 

            oNewRow[oRollUp.GetIndex()][oRollUp.GetColumn()] = NULL

        for iIndexAggr, oAggr in enumerate(self.m_voAggregate) :
                            
            vValues[iIndexAggr][iThisRollup - 1] += vValues[iIndexAggr][iThisRollup] 
                                                        
            match oAggr.GetType() :
                case enum_AggregateFunc.eSum :
                    
                    oNewRow[enum_SqlSchemaType.eAggregate.value][oAggr.GetIndex()] = vValues[iIndexAggr][iThisRollup] 

                case enum_AggregateFunc.eAvg :
                    
                    oNewRow[enum_SqlSchemaType.eAggregate.value][oAggr.GetIndex()] = vValues[iIndexAggr][iThisRollup] / viQntRow[iThisRollup]
                    
            vValues[iIndexAggr][iThisRollup] = 0
        
        viQntRow[iThisRollup - 1] += viQntRow[iThisRollup]
        viQntRow[iThisRollup] = 0

        return oNewRow

    def _RollUpManager(self, vvFullTupleSet : list) -> list :
        
        vvFullTupleResult = []

        iSize = len(self.m_voRollUp)

        vvFullTupleTemp = self._MatrixDicotomicSort(vvFullTupleSet, self.m_voRollUp, [True] * iSize)
                
        viQntRow = [0] * iSize
                        
        vValues = [None] * len(self.m_voAggregate)
        
        for ii in range(0, len(self.m_voAggregate)) :
            vValues[ii] = [0] * iSize

               
        oPivot = vvFullTupleTemp[0]
        		
        iThisRollup = iSize - 1
        
        rangeForCheck = range(0, iSize - 1)

        oNewRow = None
        
        iIndexRollupCol = 1
        
        for oTupla in vvFullTupleTemp :
                           
            oSchemaItem : SqlSchemaItem
                      
            for ii in rangeForCheck :

                oSchemaItem = self.m_voRollUp[ii] 
            
                iTable = oSchemaItem.GetIndex()
                iCol = oSchemaItem.GetColumn()

                if oTupla[iTable][iCol] != oPivot[iTable][iCol] :
                    iIndexRollupCol = iSize - ii
                    break
            
            iThisRollupTmp = iThisRollup
                                
            while iIndexRollupCol > 1 :

                oNewRow = self._rollupAddSubtotalRow(oPivot, iThisRollupTmp, iSize, vValues, viQntRow)

                vvFullTupleResult.append(oNewRow)

                iThisRollupTmp -= 1
                iIndexRollupCol -= 1
            
            self._rollupAggregateUpdater(oTupla, iThisRollup, vValues, viQntRow)

            vvFullTupleResult.append(oTupla)       

            oPivot = oTupla

        iThisRollupTmp = iThisRollup
        
        iIndexRollupCol = iSize
        
        while iIndexRollupCol > 0 :

            oNewRow = self._rollupAddSubtotalRow(oPivot, iThisRollupTmp, iSize, vValues, viQntRow)

            vvFullTupleResult.append(oNewRow)

            iThisRollupTmp -= 1
            iIndexRollupCol -= 1
        
        return vvFullTupleResult

    def _GroupByCubeParser(self, sQuery : str) :

        iIndexHnd, oHandler, sGroupingRule, sFoot = Gen.GetHandler(self, sQuery, SqlSelect.st_tSelectClause)
		
        if not sGroupingRule :
            return False, None, None, None

        sGroupingRule, bResult = Gen.RemoveTonde(sGroupingRule)

        if not bResult :
            return False, None, None, None

        vsGroupingSetRules = []

        while bResult :

            cSingleCol, sGroupingRule, bResult = Gen.Split(sGroupingRule, ',')

            cSingleCol, bResultTonde = Gen.RemoveTonde(cSingleCol)

            if not bResultTonde :
                return False, None, None, None

            vsGroupingSetRules.append(cSingleCol)

        vsRules = Gen.CreatePermuta(vsGroupingSetRules)

        sNewQuery = ''

        for sRule in vsRules :
            sNewQuery = f'{sNewQuery}( {sRule} ) ,'

        sNewQuery = f'( {sNewQuery[0:-1]} ) {sFoot if sFoot else  ""}'
        
        return self._GroupingSetsParser_1(sNewQuery)

    def _GroupingSetsParser_1(self, sQuery : str)  -> Tuple[Literal[True], int, str, str] | Tuple[Literal[False], None, None, None] :
        
        iIndexHnd, oHandler, sGroupingRule, sFoot = Gen.GetHandler(self, sQuery, SqlSelect.st_tSelectClause)
		
        if not sGroupingRule :
            return False, None, None, None

        sGroupingRule, bResult = Gen.RemoveTonde(sGroupingRule)

        if not bResult :
            return False, None, None, None

        oTableKeys = self.m_oTables.GetTableKeys()
        
        vvGroupBy = []	
                
        while sGroupingRule :
                                    
            sThisRule, sGroupingRule, bResult = Gen.Split(sGroupingRule, ',')

            sThisRule, bResult = Gen.RemoveTonde(sThisRule)
            
            if not bResult :
                return False, None, None, None

            vvGroupBy.append(sThisRule)
        
        self.m_vvGroupBy = vvGroupBy
                
        return True, iIndexHnd, sFoot, oHandler

    def _GroupingSetsParser_2(self) -> bool :
        
        vvGroupBy = []
        voRollUp = []

        for sThisRule in self.m_vvGroupBy :
            
            if sThisRule == '' :
                vvGroupBy.append([0, [SqlSchemaItem(enum_SqlSchemaType.eEmpty, -1, -1, -1, enum_DataType.eNone)]])
                continue
            
            vGroupBy = []

            vsColums = Gen.Cut(sThisRule, ',')
        
            for sColName in vsColums:
            
                oSchemaItem = self._checkForColumns(sColName)

                if not oSchemaItem :
                    self.m_oError = SqlError(enum_Error.eColumnUndefined, f'Not found any column named : "{sColName}"')
                    return False

                vGroupBy.append(oSchemaItem)

                if self.m_voRollUp :
                    voRollUp.append(oSchemaItem)
        
            vvGroupBy.append([0, vGroupBy])
        
        self.m_vvGroupBy = vvGroupBy
        self.m_voRollUp = voRollUp

        return True

    def _GroupingSetsManager(self, vvFullTupleSet : list) -> list:
              
        vvGroupBy = [None] * len(self.m_vvGroupBy)
        vvGroupFullTuple = [None] * len(self.m_vvGroupBy)
        
        oTuplaBase = []

        for vSingleTableTupla in vvFullTupleSet[0] :
            vTupla = [NULL] * len(vSingleTableTupla)
            oTuplaBase.append(vTupla)

        for oSingleFullTupla in vvFullTupleSet :
            
            for iThisGroupSet, vGroupItem in enumerate(self.m_vvGroupBy) :
                
                vGroupRule = vGroupItem[SqlSelect.THIS_GROUPBY_RULE]

                vGroupingValues = []

                oSingleGroupingTupla = deepcopy(oTuplaBase)

                for oHook in vGroupRule :
                    
                    iTabIndex = oHook.GetIndex()
                    iColIndex = oHook.GetColumn()

                    oValue = oSingleFullTupla[iTabIndex][iColIndex]

                    if oValue == 'NULL' :
                        break

                    vGroupingValues.append(oValue)

                    oSingleGroupingTupla[iTabIndex][iColIndex] = oValue

                else :
                    
                    if vvGroupBy[iThisGroupSet] is None :
                                                
                        vvGroupBy[iThisGroupSet] = [vGroupingValues]
                        vvGroupFullTuple[iThisGroupSet] = [oSingleGroupingTupla]
                        iGroupIndex = 0

                    else :
                        for iGroup, oValue in enumerate(vvGroupBy[iThisGroupSet]) :
                            if vGroupingValues == oValue :
                                iGroupIndex = iGroup
                                break
                        else :
                            
                            iGroupIndex = len(vvGroupBy[iThisGroupSet])
                            vvGroupBy[iThisGroupSet].append(vGroupingValues)
                            vvGroupFullTuple[iThisGroupSet].append(oSingleGroupingTupla)
                        
                    if self.m_voAggregate :
                        for oAggrFunc in self.m_voAggregate :
                            oAggrFunc.Evalute(oSingleFullTupla, iGroupIndex, iThisGroupSet) 
     
        vvFullTupleResult = []
        
        for iRuleIndex, vGroup in enumerate(vvGroupFullTuple) :

            self.m_vvGroupBy[iRuleIndex][SqlSelect.THIS_GROUPBY_QNT_TUPLE] = len(vGroup)
                            
            vvFullTupleResult.extend(vGroup)

        return vvFullTupleResult

    def _OrderByParser(self, sQuery : str):

        iIndexHnd, Handler, sHead, sFoot = Gen.GetHandler(self, sQuery, SqlSelect.st_tSelectClause)

        vsColums = Gen.Cut(sHead, ',')

        if vsColums == [] :
            return False, None, None, None

        self.m_voOrderBy = vsColums
        
        return True, iIndexHnd, sFoot, Handler
    
    def _OrderByManager(self) -> bool:
        
        voOrderBy = []
        vbSortingMode = []
        
        for sCol in self.m_voOrderBy :
			
            sColName, sMode, bResult = Gen.Split(sCol.strip(), ' ')
			
            sMode = sMode.strip()
			
            vbSortingMode.append(sMode != 'DESC')
			
            oSchemaItem = self.m_oSchema.GetSchema(sColName)

            if not oSchemaItem :
                self.m_oError = SqlError(enum_Error.eColumnUndefined, f'Not found any Column named : "{sColName}"')
                return False
                
            voOrderBy.append(oSchemaItem)
               
        self.m_voOrderBy = voOrderBy
        self.m_vbSortingMode = vbSortingMode

        return True

    def _HavingParser(self, sQuery : str):
        
        iIndexHnd, fuHandler, sAggregate, sFoot = Gen.GetHandler(self, sQuery, SqlSelect.st_tSelectClause)

        self.m_oHaving = sAggregate

        return True, iIndexHnd, sFoot, fuHandler

    def _JoinParser(self, sQuery : str, eMode : enum_JoinMode) :

        iIndexHnd, Handler, sHead, sFoot = Gen.GetHandler(self, sQuery, SqlSelect.st_tSelectClause)

        sTable, sExp, bResult = Gen.Split(sHead, ' ON ')

        if not bResult :
            return False, None, None, None

        sTable = sTable.strip()

        if not Gen.IsName(sTable) :
            return False, None, None, None

        if sTable not in self.m_vTablesName :
            self.m_vTablesName.append(sTable)

        self.m_voJoinRule.append(sExp)
        self.m_veJoinMode.append(eMode)

        vColumns = SqlExp().GetColumns(sExp)

        self._columnsManager(vColumns, self.m_oSchema)

        return True, iIndexHnd, sFoot, Handler
	
    def _UnionParser(self, sQuery : str):
		
        sQuery = sQuery.lstrip()

        bUnionDistinct = False
       
        sResult = Gen.IsToken(sQuery, 'DISTINCT ')

        if sResult :
            
            bUnionDistinct = True

            sUnion = sResult

        else : 

            sResult = Gen.IsToken(sQuery, 'ALL ')

            sUnion = sResult if sResult else sQuery

        sSelect = Gen.IsToken(sUnion, 'SELECT ') 
            
        if not sSelect :
            return False, None, None, None
                            
        self.m_tMultiSelect = (enum_SqlMultiSelectType.eUnion, sSelect, bUnionDistinct,)

        return True, None, None, None
	
    def _IntersectMinusParser(self, sQuery : str, eMultiSelectType : enum_SqlMultiSelectType) :
		
        sQuery = sQuery.lstrip()

        sSelect = Gen.IsToken(sQuery, 'SELECT ') 
            
        if not sSelect :
            return False, None, None, None
                            
        self.m_tMultiSelect = (eMultiSelectType, sSelect, )

        return True, None, None, None

    def _JoinManager(self, vvFullTupleSet : list, vvMatrixRecords: list) -> list :

        vvNewTupleSet = []

        vbSelectedRecord = [True] * len(vvFullTupleSet)
        
        iQntLeftRecords = 1
        iQntRightRecords = 1
        
        for iIndexRule, oJoinRule in enumerate(self.m_voJoinRule) :
            
            iQntPrevLeftRecords = iQntLeftRecords
            iQntLeftRecords = len(vvMatrixRecords[iIndexRule])
           
            iQntRightRecords = len(vvMatrixRecords[iIndexRule + 1])

            for iIndex, vSingleFullTupla in enumerate(vvFullTupleSet) :
                if vbSelectedRecord[iIndex] :

                    oResult = oJoinRule.Evalute(vSingleFullTupla)
                    
                    if oResult == None :
                        return None

                    if oResult in (False, NULL)  :
                        vbSelectedRecord[iIndex] = False

            if self.m_veJoinMode[iIndexRule] in (enum_JoinMode.eLeft, enum_JoinMode.eFull) :
                
                vbLeftJoin = [False] * iQntLeftRecords

                iTotRecord = iQntLeftRecords

                for iIndex, bState in enumerate(vbSelectedRecord) :
                    if bState :
                        
                        hh = (iIndex // iQntPrevLeftRecords) % iQntLeftRecords

                        if not vbLeftJoin[hh] :

                            vbLeftJoin[hh] = True

                            iTotRecord -= 1

                            if not iTotRecord :
                                break
                else :

                    vNullFields = [NULL] * vvMatrixRecords[iIndexRule + 1][0].Size()

                    for iIndex, bJustPresent in enumerate(vbLeftJoin) :
                        if not bJustPresent :
                            
                            vSingleFullTupla = [vvMatrixRecords[iIndexRule][iIndex].Clone()]
                            vSingleFullTupla.append(vNullFields)
                            
                            vvNewTupleSet.append(vSingleFullTupla)

            if self.m_veJoinMode[iIndexRule] in (enum_JoinMode.eRight, enum_JoinMode.eFull) :
                
                vbRightJoin = [False] * iQntRightRecords

                iTotRecord = iQntRightRecords

                for iIndex, bState in enumerate(vbSelectedRecord) :
                    if bState :

                        hh = (iIndex // iQntLeftRecords) % iQntRightRecords

                        if not vbRightJoin[hh] :
                            
                            vbRightJoin[hh] = True

                            iTotRecord -= 1

                            if not iTotRecord :
                                break

                else :

                    vNullFields = [NULL] * vvMatrixRecords[iIndexRule][0].Size()
                
                    iIndexRight = iIndexRule + 1

                    for iIndex, bJustPresent in enumerate(vbRightJoin) :
                        if not bJustPresent :
                            
                            vSingleFullTupla = [vNullFields]
                            vSingleFullTupla.append(vvMatrixRecords[iIndexRight][iIndex].Clone())
                            
                            vvNewTupleSet.append(vSingleFullTupla)


        for iIndex, bState in enumerate(vbSelectedRecord) :
            if bState :
                vvNewTupleSet.append(vvFullTupleSet[iIndex])
        
        return vvNewTupleSet

    def _makeAllTuples(self, vvMatrixRecords: list) -> list :
        
        vvFullTupleSet = self._ProdottoCartesiano(vvMatrixRecords)
        
        if self.m_oWhere :
            
            vvNewTupleSet = []

            for  vSingleFullTupla in vvFullTupleSet :

                vNodes = self.m_oWhere.GetNodes()

                for oNodo in vNodes :
                    
                    if oNodo.GetType() in (enum_ExpType.eSubQuery, enum_ExpType.eSubQueryMulti) :
                        
                        if oNodo.m_oValue == None:

                            oSqlSelect = SqlSelect(self.m_oTables)

                            oSqlTuple = oSqlSelect.Parse(oNodo.GetText())

                            if not oSqlTuple :
                                return None

                            vTuples = oSqlTuple.GetTuples()

                            if oNodo.GetType() == enum_ExpType.eSubQuery : # l'operatore legato alla subquery diverso da 'in' e 'not in' vuole un solo valore
                                if len(vTuples) == 0 or len(vTuples[0]) != 1 :
                                    return None

                                oNodo.m_oValue = vTuples[0][0]
                            
                            else :

                                oNodo.m_oValue = []

                                for oTupla in vTuples :
                                    oNodo.m_oValue.append(oTupla[0])

                if self.m_oWhere.Evalute(vSingleFullTupla) == True :
                    vvNewTupleSet.append(vSingleFullTupla)

            vvFullTupleSet = vvNewTupleSet

        if self.m_voJoinRule :
            vvFullTupleSet = self._JoinManager(vvFullTupleSet, vvMatrixRecords)

        return vvFullTupleSet

    def _AggregateFunc(self, vvFullTupleSet : list, bExternValue : bool) -> None :
        
        if self.m_vvGroupBy :
			                        
            iIndexTupla = -1

            rStep = range(len(self.m_vvGroupBy))

            for ii in rStep :
                
                rThisGroupTuple = range(self.m_vvGroupBy[ii][SqlSelect.THIS_GROUPBY_QNT_TUPLE])
                
                iGroupIndex = -1

                for jj in rThisGroupTuple :

                    vAggrValues = []
                    vRollUpValues = []

                    iGroupIndex += 1
                    iIndexTupla += 1

                    oAggr : SqlAggregate

                    for oAggr in self.m_voAggregate :
                        vAggrValues.append(oAggr.GetValue(iGroupIndex, ii))
                        vRollUpValues.append(oAggr.GetRollUp(iGroupIndex, ii))
                    
                    if bExternValue :
                        vvFullTupleSet[iIndexTupla][enum_SqlSchemaType.eAggregate.value] = vAggrValues
                        
                    else :
                        
                        vvFullTupleSet[iIndexTupla].extend([[], [], vAggrValues, []])
                    
                    vvFullTupleSet[iIndexTupla][enum_SqlSchemaType.eRollUp.value] = vRollUpValues

        else :

            for oSingleTupla in vvFullTupleSet :

                vAggrValues = []

                for oAggr in self.m_voAggregate :
                    vAggrValues.append(oAggr.GetValue(0)) 

                if bExternValue :

                    oSingleTupla[enum_SqlSchemaType.eAggregate.value] = vAggrValues

                else :
                    
                    oSingleTupla.extend([[], [], vAggrValues, []])
                     
        # end func
                             
    def _createTuples(self, vvMatrixRecords: list, bDistinct : bool) -> SqlTuple:
        
        vsLabels = []
        oSchema = SqlSchema()

        bExternValue = False

        oSchemaItem : SqlSchemaItem

        for iIndex, oSchemaItem in enumerate(self.m_voSelectedCols) :
            
            vsLabels.append(oSchemaItem.GetAlias() if oSchemaItem.GetAlias() else oSchemaItem.GetColName())
            oSchema.Append(self.m_oSchema[iIndex])
       
        oTuple = SqlTuple(vsLabels, oSchema)

        if self.m_tExists :
            
            vvFullTupleSet = self._makeExtends(vvMatrixRecords)

        else :

            vvFullTupleSet = self._makeAllTuples(vvMatrixRecords)

        if vvFullTupleSet is None :
            return None
		
        if self.m_voCase :
           
            for oSingleFullTupla in vvFullTupleSet:
                
                vCaseValue = []

                for oCase in self.m_voCase :
                    
                    oValue = oCase.Evalute(oSingleFullTupla)
                    
                    if oValue == None :
                        return None

                    vCaseValue.append(oValue)
                                
                oSingleFullTupla.extend([[], vCaseValue, [], []])

            bExternValue = True

        if self.m_voExpression :
                      
            for oSingleFullTupla in vvFullTupleSet:
                
                vExpValue = []

                for oExp in self.m_voExpression :
                    
                    oValue = oExp.Evalute(oSingleFullTupla)
                    
                    if oValue is None :
                        return None

                    vExpValue.append(oValue)

                # oSingleFullTupla.extend([vExpValue, [], []])

                if bExternValue :

                    oSingleFullTupla[enum_SqlSchemaType.eExpression.value] = vExpValue
                
                else :
                    
                    oSingleFullTupla.extend([vExpValue, [], [], []])

            bExternValue = True
        				
                
        if self.m_vvGroupBy :

            if len(self.m_vvGroupBy) == 1 :
                vvFullTupleSet = self._GroupByManager(vvFullTupleSet)
            else :
                vvFullTupleSet = self._GroupingSetsManager(vvFullTupleSet)
        
        else :

            if self.m_voAggregate :
                for oSingleFullTupla in vvFullTupleSet:
                    for oAggrFunc in self.m_voAggregate :
                        oAggrFunc.Evalute(oSingleFullTupla, 0) # nessun gruppo == un solo gruppo

        if self.m_voAggregate :
            self._AggregateFunc(vvFullTupleSet, bExternValue)
        
        if self.m_voRollUp :
            vvFullTupleSet = self._RollUpManager(vvFullTupleSet)

        if self.m_oHaving :
            
            vHavingTuple = []
            
            for oSingleTupla in vvFullTupleSet :
                if self.m_oHaving.Evalute(oSingleTupla) == True :
                    vHavingTuple.append(oSingleTupla)

            vvFullTupleSet = vHavingTuple


        if self.m_voOrderBy :
            vvFullTupleSet = self._MatrixDicotomicSort(vvFullTupleSet, self.m_voOrderBy, self.m_vbSortingMode)


        # SELECT CLAUSE
        vSelectedTuple = []
       
        viFieldsRange = range(len(self.m_voSelectedCols))
        
        iQntItems = 0
 
        for oSingleFullTupla in vvFullTupleSet :
            
            vFieldSet = []

            for iIndex in viFieldsRange :

                item = self.m_oSchema[iIndex]       

                oThisField = oSingleFullTupla[item.GetIndex()][item.GetColumn()]

                vFieldSet.append('NULL' if (oThisField == NULL) else oThisField)
               
            if bDistinct :
                for oDistinctTuple in vSelectedTuple :
                    for iIndex in viFieldsRange :
                        if vFieldSet[iIndex] != oDistinctTuple[iIndex] :
                            break
                    else : 

                        break
                else :

                    vSelectedTuple.append(vFieldSet)

                    if self.m_iLimit :
                        iQntItems += 1

                        if iQntItems == self.m_iLimit :
                            break
            else :
                
                vSelectedTuple.append(vFieldSet)

                if self.m_iLimit :
                    iQntItems += 1

                    if iQntItems == self.m_iLimit :
                        break
		
        for oSingleFullTupla in vSelectedTuple :
            oTuple.Add(oSingleFullTupla)
				
        if self.m_tMultiSelect :

            oSelect = SqlSelect(self.m_oTables)

            oSelectTuple = oSelect.Parse(self.m_tMultiSelect[1])

            if not oSelectTuple :
                return None

            oSelectSchema = oSelectTuple.GetSchema()
            
            if len(oSchema) != len(oSelectSchema) :
                return None
            
            for iIndex, item in enumerate(oSchema) :
                if item.GetType() != oSelectSchema[iIndex].GetType() :
                    return None

            if self.m_tMultiSelect[0] == enum_SqlMultiSelectType.eUnion :
                
                for oSingleFullTupla in oSelectTuple :
                    oTuple.Add(oSingleFullTupla)
                            
                if self.m_tMultiSelect[2] : # clause DISTINCT
                    vTuple = self._distinctTuple(oTuple, len(oSchema))
                    
                    oTuple.SetTuples(vTuple)	
            
            elif self.m_tMultiSelect[0] == enum_SqlMultiSelectType.eIntersect :
                
                vIntersectTuple = []
                oIntersectRange = range(len(oSchema))

                for oSingleTupla1 in oTuple :
                    for oSingleTupla2 in oSelectTuple :
                        for iIndex in  oIntersectRange :
                            if oSingleTupla1[iIndex] != oSingleTupla2[iIndex] :
                                break
                        
                        else :
                            
                            vIntersectTuple.append(oSingleTupla1)
                            break
                
                oTuple.SetTuples(vIntersectTuple)

            elif self.m_tMultiSelect[0] == enum_SqlMultiSelectType.eMinus :
                
                vMinusTuple = []
                oMinusRange = range(len(oSchema))

                for oSingleTupla1 in oTuple :
                    for oSingleTupla2 in oSelectTuple :
                        for iIndex in  oMinusRange :
                            if oSingleTupla1[iIndex] != oSingleTupla2[iIndex] :
                                break
                        
                        else :
                            
                            break
                    else :

                        vMinusTuple.append(oSingleTupla1)

                
                oTuple.SetTuples(vMinusTuple)
                        
			
        # if self.m_vOrderBy :
        #     vSelectedTuple = self._DicotomicSort(oTuple.GetTuples(), self.m_vOrderBy, self.m_bAscSorting)
        #     oTuple.SetTuples(vSelectedTuple)

        return oTuple
   
    def _DicotomicSortAsc(self, vSource : list, item, viFieldIndex : list, bUnique : bool = False, viIndex : list = None, iPos : int = None) -> bool :

        bDoInsert = True 

        oSource = vSource[0]

        for iField in viFieldIndex :
                        
            if item[iField] < oSource[iField] :
                break
           
            if oSource[iField] < item[iField] :
                bDoInsert = False
                break

        if bDoInsert == True :

            if bUnique :
                if oSource[iField] == item[iField] :
                    return False 

            vSource.insert(0, item)

            if viIndex != None :
                viIndex.insert(0, iPos)

            return True

        bDoInsert = True

        oSource = vSource[-1]

        for iField in viFieldIndex :

            if(item[iField] < oSource[iField]):
                bDoInsert = False
                break

            if(oSource[iField] < item[iField]):
                break

        if bDoInsert == True :

            if bUnique :
                if oSource[iField] == item[iField] :
                    return False 

            vSource.append(item)

            if viIndex != None :
                viIndex.append(iPos)

            return True

        iIndex = 0
        iLo = 0
        iHi = len(vSource) - 1

        while(iLo <= iHi) :

            bDoWhile = False
            
            iIndex = (iLo + iHi) // 2
 
            oSource = vSource[iIndex]

            for iField in viFieldIndex :

                if(item[iField] > oSource[iField]):
                    iLo = iIndex + 1
                    bDoWhile = True
                    break

                elif(oSource[iField] > item[iField]):
                    iHi = iIndex - 1
                    bDoWhile = True
                    break

            if (bDoWhile == False):
                break
        
        if bUnique :
            if oSource[iField] == item[iField] :
                return False 

        # vSource.insert(iIndex, item)
        vSource.insert(iLo, item)

        if viIndex != None :
            # viIndex.insert(iIndex, iPos)
            viIndex.insert(iLo, iPos)

        return True
    
    def _DicotomicSortDesc(self, vSource : list, item, viFieldIndex : list = [0], bUnique : bool = False, viIndex : list = None, iPos : int = None) -> bool :
        
        bDoInsert = True

        oSource = vSource[-1]

        for iField in viFieldIndex :

            if(item[iField] < oSource[iField]):
                break

            if(oSource[iField] < item[iField]):
                bDoInsert = False
                break

        if bDoInsert == True :
            
            if bUnique :
                if oSource[iField] == item[iField] :
                    return False 
            
            vSource.append(item)

            if viIndex != None :
                viIndex.append(iPos)
            
            return True

        bDoInsert = True 

        oSource = vSource[0]

        for iField in viFieldIndex :
                        
            if(item[iField] > oSource[iField]):
                break
           
            if(oSource[iField] > item[iField]):
                bDoInsert = False
                break

        if bDoInsert == True :

            if bUnique :
                if oSource[iField] == item[iField] :
                    return False 

            vSource.insert(0, item)

            if viIndex != None :
                viIndex.insert(0, iPos)

            return True

        iIndex = 0
        iLo = 0
        iHi = len(vSource) - 1

        while(iLo <= iHi) :

            bDoWhile = False
            
            iIndex = (iLo + iHi) // 2
 
            oSource = vSource[iIndex]

            for iField in viFieldIndex :

                if(item[iField] < oSource[iField]):
                    iLo = iIndex + 1
                    bDoWhile = True
                    break

                elif(oSource[iField] < item[iField]):
                    iHi = iIndex - 1
                    bDoWhile = True
                    break

            if (bDoWhile == False):
                break

        if bUnique :
            if oSource[iField] == item[iField] :
                return False 
        
        # vSource.insert(iIndex, item)
        vSource.insert(iLo, item)

        if viIndex != None :
            # viIndex.insert(iIndex, iPos)
            viIndex.insert(iLo, iPos)

        return True

    def _DicotomicSort(self, vSource, viFieldIndex : list, bAscMode : bool = True):
        
        if vSource == [] :
            return []

        vResult = [vSource[0]]

        vArray = vSource[1:]

        if bAscMode == True:
            for item in vArray :
                self._DicotomicSortAsc(vResult, item, viFieldIndex)
        else:
            
            for item in vArray :
                self._DicotomicSortDesc(vResult, item, viFieldIndex)

        return vResult
	
    def _sortCompareRule(self, it1, it2, iResult : int) -> int :
		
        if isinstance(it1, SqlNull) :
            return -iResult
			
        if isinstance(it2, SqlNull) :
            return iResult
			
        if it1 < it2 :
            return -iResult
			
        if it1 > it2 :
            return iResult
		
        return 0
	
    def __matrixDicotomicSortAsc(self, vvSource : list, item, voSchema : list, viSortingMode : list, bUnique : bool = False, viIndex : list = None, iPos : int = None) -> bool :
        
        if not vvSource :
            vvSource.append(item)
            
            if viIndex != None :
                viIndex.insert(0, iPos)

            return True

        bDoInsert = True 

        vSource = vvSource[0]

        oSchema : SqlSchemaItem

        for iIndex, oSchema in enumerate(voSchema) :
            
            iTable = oSchema.GetIndex()
            iColumn = oSchema.GetColumn()
									
            iCompareResult = self._sortCompareRule(item[iTable][iColumn], vSource[iTable][iColumn], viSortingMode[iIndex])
			
            if iCompareResult == -1 :
                break
           
            if iCompareResult == 1 :
                bDoInsert = False
                break

        if bDoInsert == True :

            if bUnique :
                if iCompareResult == 0 :
                    return False 

            vvSource.insert(0, item)

            if viIndex != None :
                viIndex.insert(0, iPos)

            return True

        bDoInsert = True

        vSource = vvSource[-1]

        for iIndex, oSchema in enumerate(voSchema) :

            iTable = oSchema.GetIndex()
            iColumn = oSchema.GetColumn()
			
            iCompareResult = self._sortCompareRule(item[iTable][iColumn], vSource[iTable][iColumn], viSortingMode[iIndex])
			
            if iCompareResult == -1 :
                bDoInsert = False
                break

            if iCompareResult == 1 :
                break

        if bDoInsert == True :

            if bUnique :
                if iCompareResult == 0 :
                    return False 

            vvSource.append(item)

            if viIndex != None :
                viIndex.append(iPos)

            return True

        iMid = 0
        iLo = 0
        iHi = len(vvSource) - 1

        while(iLo <= iHi) :
            
            iMid = (iLo + iHi) // 2
 
            vSource = vvSource[iMid]

            for iIndex, oSchema in enumerate(voSchema) :
    
                iTable = oSchema.GetIndex()
                iColumn = oSchema.GetColumn()
				
                iCompareResult = self._sortCompareRule(item[iTable][iColumn], vSource[iTable][iColumn], viSortingMode[iIndex])
								
                if iCompareResult == 1 :
                    iLo = iMid + 1
                    break

                if iCompareResult == -1 :
                    iHi = iMid - 1
                    break

            else : 
                
                iLo = iMid
                break #end while
        
        if bUnique :
            if iCompareResult == 0 :
                return False 
        
        vvSource.insert(iLo, item)

        if viIndex != None :
            viIndex.insert(iLo, iPos)

        return True

    def _MatrixDicotomicSort(self, vvSource : list, voSchema : list, vbAscMode : list):
        
        vvResult = []
        viSortingMode = []
        
        for bAsc in vbAscMode :
            viSortingMode.append(1 if bAsc else -1)
			
        for item in vvSource :
	        self.__matrixDicotomicSortAsc(vvResult, item, voSchema, viSortingMode)
    
        return vvResult

    def _CountHnd(self, sArgs : str, *Nop) -> SqlAggregate and list:
        
        bDistinct = False
                
        sHead, sExp, bResult = Gen.Split(sArgs, 'DISTINCT ')

        if bResult :

            if sHead :
                return None
            
            bDistinct = True
        
        else :

            sExp = sArgs


        oSqlAggregate = SqlAggregate(enum_AggregateFunc.eCount, sExp, bDistinct)
        
        return oSqlAggregate, SqlExp().GetColumns(sExp)
    
    def _AggrGenericHnd(self, sExp : str, eAggregateFunc : enum_AggregateFunc) -> SqlAggregate :
        
        oSqlAggregate = SqlAggregate(eAggregateFunc, sExp)
        
        return oSqlAggregate, SqlExp().GetColumns(sExp)

    def _distinctTuple(self, vvSource : list, viFieldToCompare : list or int) -> list :
        
        viToCompare = range(viFieldToCompare) if isinstance(viFieldToCompare, int) else viFieldToCompare

        vvTargetTmp = []
        viPosition = []

        for iIndex, item in enumerate(vvSource) :
            self._DicotomicSortAsc(vvTargetTmp, item, viToCompare, True, viPosition, iIndex) 

        iQntItem = len(vvTargetTmp)
 
        if len(vvSource) == iQntItem :
            return deepcopy(vvSource)

        vvTmp = [None] * iQntItem

        for iIndex, item in enumerate(vvTargetTmp) :
            vvTmp[viPosition[iIndex]] = item

        vvTarget = []

        for item in vvTmp :
            if item :
                vvTarget.append(item)

        return vvTarget

    def _HavingManager(self) -> bool:

        oSqlExp = SqlExp()

        # oSqlExp.Parse(self.m_oHaving, bNodeList = True)
        if not oSqlExp.Parse(self.m_oHaving, self.m_oSchema, True) :
            return False

        for oNodo in oSqlExp.m_vNodeList :
            if oNodo.GetAggrType() :
                for oAggr in self.m_voAggregate :
                    if oNodo.GetText() == oAggr.GetText() :
                        oNodo.SetItem(oAggr.GetIndex())
                        break
                else :
                                        
                    iIndex, fuAggrHnd, sFunc, sArgs = Gen.GetHandler(self, ' ' + oNodo.GetText(), SqlSelect.st_tSelectAggregate)

                    if iIndex == None :
                        return False

                    oAggregate, vCols = fuAggrHnd(sArgs, oNodo.GetAggrType())

                    if not oAggregate :
                         return False

                    iIndex = len(self.m_voAggregate)

                    oNodo.SetItem(iIndex)
                    oAggregate.SetIndex(iIndex)                                                       
                    
                    self.m_voAggregate.append(oAggregate)

                    self._columnsManager(vCols, self.m_oSchema) 
        
        self.m_oHaving = oSqlExp

        return True

    def _ProdottoCartesiano(self, vvMatrixRecords, viThisTables : list = None) :
        
        vvFullTupleSet = []

        if viThisTables :
            
            vvMatriceDiRecords = []

            for iIndex in viThisTables :
                vvMatriceDiRecords.append(vvMatrixRecords[iIndex])

        else :

            vvMatriceDiRecords = vvMatrixRecords


        for vAllRecordsSingleTable in vvMatriceDiRecords:
            
            vvNewTupleSet = []
            
            for oRecord in vAllRecordsSingleTable:
                
                vFields = oRecord.Clone()

                if  not vvFullTupleSet :
                    vvNewTupleSet.append([vFields])

                else:
                                        
                    for oTupla in vvFullTupleSet: # prodotto cartesiano tra tabelle

                        vSingleFullTupla = deepcopy(oTupla)

                        vSingleFullTupla.append(vFields[:])
                        
                        vvNewTupleSet.append(vSingleFullTupla)
        
            vvFullTupleSet = vvNewTupleSet

        return vvFullTupleSet
    
    def _makeExtends(self, vvMatrixRecords: list) -> list :
        
        viMainTables = []
        viSubTables = []

        for sTable in self.m_tExists[0] :
            iIndex = Gen.Inside(sTable, self.m_vTablesName)

            if iIndex == -1 :
                return None
            
            viMainTables.append(iIndex)
        
        for sTable in self.m_tExists[1] :
            iIndex = Gen.Inside(sTable, self.m_vTablesName)

            if iIndex == -1 :
                return None
            
            viSubTables.append(iIndex)


        vBaseTupleSet = self._ProdottoCartesiano(vvMatrixRecords, viMainTables)
		
        if not self.m_oWhere :
            return vBaseTupleSet
		
       		
        iQntRecordBase = len(vBaseTupleSet)
        
        viQntRecords = []
        viQntRecords.append(iQntRecordBase)

        vvSubRecords = []

        for iIndex in viSubTables :
            
            vFieldSet = []

            for oRecord in vvMatrixRecords[iIndex] :
                vFieldSet.append(oRecord.GetFieldSet())
            
            vvSubRecords.append(vFieldSet)
            viQntRecords.append(len(vFieldSet))
		
        iQntTables = len(vvSubRecords) + 1 # +1 per la base
				
        viIndexRecord = [0] * iQntTables
		
        vvFullTupleSet = []
		
        while(viIndexRecord[0] < iQntRecordBase) :
            
            vvSingleTupleSet = deepcopy(vBaseTupleSet[viIndexRecord[0]])

            # vvSingleTupleSet.append(vBaseTupleSet[viIndexRecord[0]])

            iIndexTable = 0

            for vThisTable in vvSubRecords :

                iIndexTable += 1

                iThisRecord = viIndexRecord[iIndexTable]
                vvSingleTupleSet.append(vThisTable[iThisRecord])
			
            if self.m_oWhere.Evalute(vvSingleTupleSet) == True :
                vvFullTupleSet.append(vBaseTupleSet[viIndexRecord[0]])
				
                viTmp = [0] * iQntTables
                viTmp[0] = viIndexRecord[0] + 1
                viIndexRecord = viTmp
                continue	
					
			# gestione indici per realizzare il prodotto cartesiano				
            viIndexRecord[iIndexTable] += 1

            while(iIndexTable > 0) :
                
                if viIndexRecord[iIndexTable] < viQntRecords[iIndexTable] :
                    break
                    
                viIndexRecord[iIndexTable] = 0
                iIndexTable -= 1
                viIndexRecord[iIndexTable] += 1
                    
        return vvFullTupleSet

    def _CaseParser(self, sQuery : str) -> SqlCase :
        oSqlCase = SqlCase()

        if not oSqlCase.Parse(sQuery) :
            self.m_oError = oSqlCase.GetError()
            return None

        self.m_voCase.append(oSqlCase)

        return oSqlCase
