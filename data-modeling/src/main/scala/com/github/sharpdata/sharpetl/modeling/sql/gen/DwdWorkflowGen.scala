package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.model.{CreateDimMode, DimTable, DwdModeling, DwdTableConfig, FactOrDim}
import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.syntax.{Workflow, WorkflowStep}
import com.github.sharpdata.sharpetl.core.util.Constants.LoadType._
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType._
import com.github.sharpdata.sharpetl.modeling.excel.model._
import AutoCreateDimSqlGen._
import AutoCreateDimSqlGen2.genAutoCreateDimStep
import DwdExtractSqlGen._
import DwdLoadSqlGen.genLoadStep
import DwdTransformSqlGen.genTargetSelectStep
import DwdTransformSqlGen2._
import ScdSqlGen._

// scalastyle:off
object DwdWorkflowGen {
  def genWorkflow(dwdModding: DwdModeling, workflowName: String): Workflow = {
    var steps: List[WorkflowStep] = List.empty
    var index = 1
    val dwdTableConfig: DwdTableConfig = dwdModding.dwdTableConfig
    //    1. 【创建step，target:temp】：从上一层获取后去数据，目标：temp
    //    1.1 行filter
    //    1.2 添加多余的列
    //    注： 此时，不进行列filter，后续step需要使用
    steps = genExtractStep(dwdModding, index)
    index += 1

    //    2. 和主数据关联
    //    2.1 判断create_dim_mode
    val createDimModeToCols = dwdModding.columns.filterNot(it => isNullOrEmpty(it.joinTable)).groupBy(_.createDimMode)

    //    2.1.1 always ：【创建step，target:postgres】：
    //      基于business_create_time进行升序排序并选取join_table_column中指定的列进行去重，之后每条数据依次进行渐变（注：需要标记该数据来自于维度表）
    //    2.1.2 once ：【创建step，target:postgres】：
    //      基于business_create_time进行升序排序选取第一条数据，并选取join_table_column中指定的列，之后将该条数据insert到dim表中，（注：check该条数据是否存在，同时需要标记该数据来自于维度表）
    val partitionCols = dwdModding.columns.filter(_.partitionColumn)
    val updateTimeCols = dwdModding.columns.filter(_.businessUpdateTime)
    val createTimeCols = dwdModding.columns.filter(_.businessCreateTime)
    val nonNeverCreateDimTables: Seq[DimTable] = (
      createDimModeToCols
        .getOrElse(CreateDimMode.ALWAYS, Seq()) ++
        createDimModeToCols
          .getOrElse(CreateDimMode.ONCE, Seq())
      )
      .groupBy(_.joinTable)
      .map { case (dimTable, cols) => DimTable(dimTable, cols, partitionCols, updateTimeCols, createTimeCols) }
      .toSeq

    if (nonNeverCreateDimTables.nonEmpty && dwdTableConfig.targetType == HIVE) {
      val extractTempTableName = steps.head.target.asInstanceOf[DBDataSourceConfig].tableName
      val sql = genGroupedDimensionSql(nonNeverCreateDimTables, extractTempTableName, dwdTableConfig.sourceTable)
      steps = tmpStepToTempStep(steps, index, extractTempTableName, "grouped_dim", sql)
      index += 1

      val groupedDimTempTable = steps.last.target.asInstanceOf[DBDataSourceConfig].tableName

      nonNeverCreateDimTables.sortBy(_.dimTable).foreach { dimTable =>
        steps = tmpStepToTempStep(steps, index, groupedDimTempTable, "selected_dim", parseDimensionSql(groupedDimTempTable, dimTable))
        val dimView = steps.last.target.asInstanceOf[DBDataSourceConfig].tableName
        index += 1
        steps = genDwdPartitionClauseStep(steps, dimTable, index)
        index += 1
        steps = genDwdViewStep(steps, dimTable, index)
        val dwView = steps.last.target.asInstanceOf[DBDataSourceConfig].tableName
        index += 1
        steps = genScdStep(steps, dwdModding, dimTable, index, dimView, dwView)
        index += 1
      }
    }

    //    2.2 和主数据关联【创建step，target:temp】，获取id信息，基于source_column和join_on的对应关系，
    //      和对应的join_table表进行join，同时基于业务时间business_create_time进行维度表的start_time和end_time进行join
    //      获取join_table_column中对应target_column中extra_column_expression标识为'zip_id_flag'的值，此时zipId的命名以target_column列为准
    //      注：这一步仅仅是获取各个维度数据的zipId
    if (dwdTableConfig.targetType != HIVE) {
      val (autoCreateDimTableSteps, autoCreateDimNextTempIndex) = genAutoCreateDimStep(steps, dwdModding, index)
      steps = autoCreateDimTableSteps
      index = autoCreateDimNextTempIndex
      val (readMatchTableSteps, nextTempIndex) = generateReadMatchTableStep(steps, dwdModding, index)
      steps = readMatchTableSteps
      index = nextTempIndex
    }

    val (matchStep, nextIndex) = if (dwdTableConfig.targetType == HIVE) genMatchStepInHive(steps, dwdModding, index) else genMatchStep(steps, dwdModding, index)
    steps = matchStep
    index = nextIndex

    //    3. 质量check
    //    3.1 逻辑主键缺失check【创建step，target: temp】，此时判断logic_primary_column的逻辑主键中是否包含维度数据（判断逻辑为logic_primary_column中为true的配置列join_table是否有值）
    //    3.1.1 逻辑主键中不包括维度数据。check逻辑主键列的null判断
    //    3.1.2 逻辑主键中包括维度数据。check逻辑主键中非维度列进行null判断，维度列进行-1判断（注：定义清楚是-1还是'-1'）
    //    3.2 逻辑主键缺失数据入库【创建step，target: postgres】
    //    3.3 逻辑主键缺失数据删除【创建step， target: temp】
    //    3.4 逻辑主键重复check【创建step，target: temp】
    //    3.5 逻辑主键重复数据入库【创建step，target: postgres】
    //    3.6 逻辑主键重复数据删除【创建step， target: temp】
    //    3.7 主要字段缺失数据check+入库【该配置目前没有，也可看做自定义业务扩展，无需配置】
    //    3.8 和主数据关联不上的check【创建step，target: temp】(注：目前不考虑事实表和事实表关联的check)
    //    3.9 和主数据关联不上数据入库【创建step，target: postgres】

    //    4. 常规逻辑处理
    //    4.1 删除多余的列【创建step，target: temp】
    steps = genTargetSelectStep(steps, dwdModding, index)
    index += 1

    //    5. load【场景比较复杂，暂时先考虑如下备注中的场景】
    //    5.1 场景：全量+渐变,获取渐变数据delete/update/insert，并基于逻辑主键渐变入库【创建step，target:xxx】
    //    5.2 场景：全量+非渐变,truncate+insert入库【创建step，target:xxx】
    //    5.3 场景：增量+渐变,基于逻辑主键渐变入库【创建step，target:xxx】
    //    5.4 场景：增量+非渐变,及与逻辑主键update入库【创建step，target:xxx】

    // HIVE/INCREMENTAL/DIM/slowChanging
    if (dwdTableConfig.targetType == HIVE && dwdTableConfig.updateType == INCREMENTAL
      && dwdTableConfig.factOrDim == FactOrDim.DIM && dwdTableConfig.slowChanging) {
      steps = genDwdPartitionClauseStep(steps, dwdModding, index)
      index += 1
      steps = genDwdViewStep(steps, dwdModding, index)
      index += 1
      steps = genScdStep(steps, dwdModding, index)
      index += 1
    } // HIVE/INCREMENTAL/FACT/slowChanging
    else if (dwdTableConfig.targetType == HIVE && dwdTableConfig.updateType == INCREMENTAL
      && dwdTableConfig.factOrDim == FactOrDim.FACT && dwdTableConfig.slowChanging) {
      steps = genDwdPartitionClauseStep(steps, dwdModding, index)
      index += 1
      steps = genDwdViewStep(steps, dwdModding, index)
      index += 1
      steps = genScdStep(steps, dwdModding, index)
      index += 1
    } // HIVE/INCREMENTAL/FACT/no slowChanging
    else if (dwdTableConfig.targetType == HIVE && dwdTableConfig.updateType == INCREMENTAL
      && dwdTableConfig.factOrDim == FactOrDim.FACT && !dwdTableConfig.slowChanging) {
      steps = genDwdPartitionClauseStep(steps, dwdModding, index, isSCD = false)
      index += 1
      steps = genDwdViewStep(steps, dwdModding, index)
      index += 1
      steps = genScdStep(steps, dwdModding, index, isSCD = false)
      index += 1
    }
    else {
      steps = genLoadStep(steps, dwdModding, index)
    }

    // scalastyle:off
    Workflow(workflowName, "", dwdTableConfig.updateType,
      "upstream", //TODO: update later
      s"ods__${dwdModding.dwdTableConfig.sourceTable}", null, null, 0, null, false, null, Map(), steps
    )
    // scalastyle:on

    /**
     * load逻辑备注
     * 问题：指定时间窗口的全量数据是全量么？如果是增量，就意味着要基于逻辑主键修改（这样做有问题），如果是全量，可以基于时间窗口修改
     * 迟到维的问题
     * 全量（真正的全量）（如果是不同逻辑的全量，需要先将数据补充成全量数据，自定义扩展）
     * 渐变
     * 基于业务的update_time（表中没有使用create_time）进行渐变。注意：如果待更新的数据时间大于表中的最大start_time，直接报错（还是取消更新，邮件告警？如果取消，取消粒度是单个数据还是批次？）
     * 不渐变
     * truncate全表，insert 新数据（可以暂时不用考虑更新时间小于表中更新时间的情况，否则又会引出这批全量数据导出时间的概念，配置会更复杂）
     * 增量
     * 渐变
     * 根据逻辑主键基于业务的update_time（表中没有使用create_time）进行渐变。注意：如果发现待更新的数据时间大于表中的最大start_time，直接报错（还是取消更新，邮件告警？如果取消，取消粒度是单个数据还是批次？）
     * 不渐变
     * 根据逻辑主键基于业务的update_time进行更新，如果更新的数据时间大于表中的更新时间，取消更新（防止旧数据覆盖新数据）
     */
    //    TODO 建议step拆分到比较细的粒度，方便后续自定义sql的修改，同时step的拆分目的是结果temp可以在后续step的复用。该规范待确认
  }
}
