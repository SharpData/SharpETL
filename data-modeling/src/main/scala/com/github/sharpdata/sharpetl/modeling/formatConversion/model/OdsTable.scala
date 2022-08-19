package com.github.sharpdata.sharpetl.modeling.formatConversion.model

object OdsTable {

  final case class OdsModelingColumnParietal(sourceTable: String,
                                             targetTable: String,
                                             sourceColumn: String,
                                             targetColumn: String,
                                             sourceType: String,
                                             extraColumnExpression: String,
                                             incrementalColumn: Boolean,
                                             primaryKeyColumn: Boolean,
                                             isNullAbel:Boolean
                                    )

  final case class OdsTypeModelingColumn(sourceTable: String,
                                             targetTable: String,
                                             sourceColumn: String,
                                             targetColumn: String,
                                             sourceType: String,
                                             targetType: String,
                                             extraColumnExpression: String,
                                             incrementalColumn: Boolean,
                                             primaryKeyColumn: Boolean,
                                             isNullAbel:Boolean
                                            )

}
