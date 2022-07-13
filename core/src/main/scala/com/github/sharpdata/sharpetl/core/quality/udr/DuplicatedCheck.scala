package com.github.sharpdata.sharpetl.core.quality.udr

import com.github.sharpdata.sharpetl.core.quality.QualityCheck.joinIdColumns
import com.github.sharpdata.sharpetl.core.quality.{DataQualityConfig, UserDefinedRule}

object DuplicatedCheck extends UserDefinedRule {
  override def check(tempViewName: String, idColumn: String, udr: DataQualityConfig): (String, String) = {
    val resultViewName = s"${tempViewName}__${udr.dataCheckType.replace(' ', '_')}__${udr.column}"
    val sql =
      s"""|CREATE TEMPORARY VIEW $resultViewName
          |          (ID COMMENT 'duplicated id')
          |          AS SELECT ${joinIdColumns(idColumn, prefix = "a")} AS id
          |          FROM `$tempViewName` a
          |          INNER JOIN (SELECT `$tempViewName`.`${udr.column}`
          |                     FROM `$tempViewName`
          |                     GROUP BY `$tempViewName`.`${udr.column}`
          |                     HAVING count(*) > 1) b
          |                ON a.`${udr.column}` = b.`${udr.column}`
          |""".stripMargin
    (sql, resultViewName)
  }
}
