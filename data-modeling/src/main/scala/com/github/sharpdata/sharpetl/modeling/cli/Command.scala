package com.github.sharpdata.sharpetl.modeling.cli

import com.github.sharpdata.sharpetl.modeling.excel.parser.DwdTableParser
import com.github.sharpdata.sharpetl.core.cli.{BatchJobCommand, CommonCommand}
import com.github.sharpdata.sharpetl.core.util.IOUtil.getFullPath
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, IOUtil}
import com.github.sharpdata.sharpetl.modeling.formatConversion.createSqlParser.createTableList
import com.github.sharpdata.sharpetl.modeling.sql.gen.DwdWorkflowGen.genWorkflow
import picocli.CommandLine

import java.io.{BufferedWriter, File, FileWriter}

@CommandLine.Command(name = "generate-ods-sql")
class GenerateSqlFiles extends CommonCommand {
  @CommandLine.Option(
    names = Array("-f", "--file"),
    description = Array("Excel file path"),
    required = true
  )
  var filePath: String = _

  @CommandLine.Option(
    names = Array("-h", "--help"),
    usageHelp = true,
    description = Array("Sample parameters: -f=/path/to/config.xlsx")
  )
  var helpRequested = false

  @CommandLine.Option(
    names = Array("--output"),
    required = true,
    description = Array("Write to sql file path")
  )
  var output: String = _

  override def formatCommand(): Unit = {
    commandStr.append(s"--file=$filePath \t")
    commandStr.append(s"--output=$output \t")
    commandStr.append(s"--help=$helpRequested \t")
    super.formatCommand()
  }

  override def run(): Unit = {
    loggingJobParameters()
    import com.github.sharpdata.sharpetl.modeling.excel.parser.OdsTableParser
    import com.github.sharpdata.sharpetl.modeling.sql.gen.OdsWorkflowGen
    val odsModelings = OdsTableParser.readOdsConfig(filePath)
    odsModelings
      .foreach(modeling => {
        val workflowName = s"ods__${modeling.odsTableConfig.targetTable}"
        val workflow = OdsWorkflowGen.genWorkflow(modeling, workflowName)
        writeFile(workflowName, workflow.toString)
      })
  }

  def writeFile(filename: String, sqlContent: String): Unit = {
    val path = getFullPath(output)
    val file = new File(s"$path/$filename.sql")
    ETLLogger.info(s"Write sql file to $file")
    val sqlWriter = new BufferedWriter(new FileWriter(file))
    sqlWriter.write(sqlContent)
    sqlWriter.close()
  }
}

@CommandLine.Command(name = "generate-dwd-sql")
class GenerateDwdStepCommand extends BatchJobCommand {

  @CommandLine.Option(names = Array("--output"), description = Array("Write to sql file path"))
  var outputPath: String = _

  override def formatCommand(): Unit = {
    commandStr.append(s"--output=$outputPath \t")
    super.formatCommand()
  }


  override def run(): Unit = {
    loggingJobParameters()
    val tables = DwdTableParser.readDwdConfig(excelOptions.filePath)

    val path = getFullPath(outputPath)

    tables
      .foreach(table => {
        val workflowName = s"${table.dwdTableConfig.sourceTable}_${table.dwdTableConfig.targetTable}"
        val workflow = genWorkflow(table, workflowName)
        val file = s"$path/$workflowName.sql"
        ETLLogger.info(s"Write sql file to $file")
        IOUtil.write(
          path = file,
          line = workflow.toString
        )
      })
  }
}

@CommandLine.Command(name = "generate-ods-sql—automate-generate")
class GenerateSqlAutomateGenerateFiles extends CommonCommand {
  @CommandLine.Option(
    names = Array("-f", "--file"),
    description = Array("Excel file path"),
    required = true
  )
  var filePath: String = _

  @CommandLine.Option(
    names = Array("-h", "--help"),
    usageHelp = true,
    description = Array("Sample parameters: -f=/path/to/config.xlsx")
  )
  var helpRequested = false

  @CommandLine.Option(
    names = Array("--output"),
    required = true,
    description = Array("Write to sql file path")
  )
  var output: String = _

  override def formatCommand(): Unit = {
    commandStr.append(s"--file=$filePath \t")
    commandStr.append(s"--output=$output \t")
    commandStr.append(s"--help=$helpRequested \t")
    super.formatCommand()
  }

  override def run(): Unit = {
    loggingJobParameters()
    val createSql = createTableList(filePath)
    createSql.foreach(it => {
      val workflowName = s"create_${it._1._2}"
      writeFile(workflowName, it._2)
    })
  }


  def writeFile(filename: String, sqlContent: String): Unit = {
    val path = getFullPath(output)
    val file = new File(s"$path/$filename.sql")
    ETLLogger.info(s"Write sql file to $file")
    val sqlWriter = new BufferedWriter(new FileWriter(file))
    sqlWriter.write(sqlContent)
    sqlWriter.close()
  }
}
