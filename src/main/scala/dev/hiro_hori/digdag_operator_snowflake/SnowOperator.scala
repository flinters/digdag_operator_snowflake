package dev.hiro_hori.digdag_operator_snowflake

import io.digdag.spi.{Operator, OperatorContext, OperatorFactory, TaskExecutionException, TaskResult, TemplateEngine}
import io.digdag.util.BaseOperator
import org.slf4j.LoggerFactory
import com.fasterxml.jackson.databind.ObjectMapper
import io.digdag.client.config.Config
import net.snowflake.client.jdbc.{SnowflakeDriver, SnowflakeSQLException}

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties
import java.sql.{Connection, DriverManager}


// オペレータ本体
class SnowOperator(_context: OperatorContext, templateEngine: TemplateEngine) extends BaseOperator(_context) {
  private[this] val logger = LoggerFactory.getLogger(classOf[SnowOperator])

  override def runTask(): TaskResult = {
    val config = this.request.getConfig

    // Configの内容をJSONのPrettyPrintで出力
    val pretty = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(config)
    logger.debug(pretty)

    val data = try {
      workspace.templateCommand(templateEngine, config, "query", UTF_8)
    } catch {
      case e: Throwable => throw new TaskExecutionException(e)
    }

    val createTable = getOptionalParameterFromOperatorParameter(config, "create_table")
    val createOrReplaceTable = getOptionalParameterFromOperatorParameter(config, "create_or_replace_table")
    val createTableIfNotExists = getOptionalParameterFromOperatorParameter(config, "create_table_if_not_exists")
    val insertInto = getOptionalParameterFromOperatorParameter(config, "insert_into")
    if (Seq(createTable, createOrReplaceTable, createTableIfNotExists, insertInto).count(_.isDefined) >= 2) {
      throw new TaskExecutionException("you must specify only 1 option in (create_table, create_or_replace_table, create_table_if_not_exists, insert_into)")
    }
    val sql = (
      createTable,
      createOrReplaceTable,
      createTableIfNotExists,
      insertInto
    ) match {
      case (Some(table), _, _, _) => s"CREATE TABLE $table AS " + data
      case (_, Some(table), _, _) => s"CREATE OR REPLACE TABLE $table AS " + data
      case (_, _, Some(table), _) => s"CREATE TABLE $table IF NOT EXISTS AS " + data
      case (_, _, _, Some(table)) => s"INSERT INTO $table " + data
      case _ => data
    }
    logger.info(sql)

    val conn = getConnection(
      getConfigFromOperatorParameterOrExportedParameter(config, "host"),
      getConfigFromOperatorParameterOrExportedParameter(config, "user"),
      this.context.getSecrets.getSecret("snow.password"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "database"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "schema"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "warehouse"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "role"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "query_tag"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "timezone"),
      (
        getConfigFromOperatorParameterOrExportedParameterOptional(config, "session_unixtime_sql_variable_name"),
        getConfigFromOperatorParameterOrExportedParameter(config, "session_unixtime"),
      )
    )
    val stmt = conn.createStatement()
    try {
      stmt.execute(sql)
      // オペレータの処理が無事成功した場合はTaskResultを返す
      TaskResult.empty(this.request)
    } catch {
      case e: Throwable => throw new TaskExecutionException(e)
    } finally {
      conn.close()
    }
  }

  def getConnection(
                     host: String,
                     user: String,
                     password: String,
                     database: Option[String],
                     schema: Option[String],
                     warehouse: Option[String],
                     role: Option[String],
                     queryTag: Option[String],
                     timezone: Option[String],
                     unixtimeSetting: (Option[String], String),
                   ): Connection = {
    DriverManager.registerDriver(
      new SnowflakeDriver()
    )

    val prop = new Properties()
    prop.put("user", user)
    prop.put("password", password)
    database.foreach(x => prop.put("db", x))
    schema.foreach(x => prop.put("schema", x))
    warehouse.foreach(x => prop.put("warehouse", x))
    role.foreach(x => prop.put("role", x))
    queryTag.foreach(x => prop.put("query_tag", x))
    timezone.foreach(x => prop.put("timezone", x))
    unixtimeSetting._1.foreach(x => prop.put("$" + x, unixtimeSetting._2))
    //    logger.debug(prop.toString)
    try {
      DriverManager.getConnection(s"jdbc:snowflake://${host}", prop)
    } catch {
      case e: Throwable =>
        throw new TaskExecutionException(e)
    }
  }

  def getOptionalParameterFromOperatorParameter(config: Config, configName: String): Option[String] =
    Option(config.getOptional(configName, classOf[String]).orNull())

  def getConfigFromOperatorParameterOrExportedParameter(config: Config, configName: String): String =
    Option(config.getOptional(configName, classOf[String]).orNull())
      .getOrElse(config.getNested("snow").get(configName, classOf[String]))

  def getConfigFromOperatorParameterOrExportedParameterOptional(config: Config, configName: String): Option[String] = {

    val o0: Option[String] = Option(config.getOptional(configName, classOf[String]).orNull())
    val o1: Option[String] = Option(config.getNested("snow").getOptional(configName, classOf[String]).orNull())
    if (o0.isDefined) {
      o0
    } else {
      o1
    }
  }
}

// オペレータを生成するファクトリクラス
class SnowOperatorFactory(
                           templateEngine: TemplateEngine,
                         ) extends OperatorFactory {
  // ↓ これがオペレータの名前になる
  override def getType: String = "snow"

  override def newOperator(context: OperatorContext): Operator = new SnowOperator(context, templateEngine)
}