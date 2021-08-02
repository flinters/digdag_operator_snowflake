package dev.hiro_hori.digdag_operator_snowflake

import com.fasterxml.jackson.databind.ObjectMapper
import io.digdag.client.config.Config
import io.digdag.spi.*
import io.digdag.util.BaseOperator
import net.snowflake.client.jdbc.{SnowflakeDriver, SnowflakeSQLException}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{Connection, DriverManager}
import java.util.Properties

// オペレータ本体
class SnowOperator(_context: OperatorContext, templateEngine: TemplateEngine) extends BaseOperator(_context) {
  private[this] val logger = LoggerFactory.getLogger(classOf[SnowOperator])

  override def runTask(): TaskResult = {
    val config = this.request.getConfig

    // Configの内容をJSONのPrettyPrintで出力
    val pretty = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(config)
    logger.debug(pretty)

    val command = config.get("_command", classOf[String])
    val data =
      try {
        workspace.templateFile(templateEngine, command, UTF_8, config)
      } catch {
        case e: Throwable => throw new TaskExecutionException(e)
      }

    val createTable = config.getOptionString("create_table")
    val createOrReplaceTable = config.getOptionString("create_or_replace_table")
    val createTableIfNotExists = config.getOptionString("create_table_if_not_exists")
    val insertInto = config.getOptionString("insert_into")
    if (Seq(createTable, createOrReplaceTable, createTableIfNotExists, insertInto).count(_.isDefined) >= 2) {
      throw new TaskExecutionException(
        "you must specify only 1 option in (create_table, create_or_replace_table, create_table_if_not_exists, insert_into)"
      )
    }
    val sql = (
      config.getOptionString("create_table"),
      config.getOptionString("create_or_replace_table"),
      config.getOptionString("create_table_if_not_exists"),
      config.getOptionString("insert_into")
    ) match {
      case (Some(table), _, _, _) => s"CREATE TABLE $table AS " + data
      case (_, Some(table), _, _) => s"CREATE OR REPLACE TABLE $table AS " + data
      case (_, _, Some(table), _) => s"CREATE TABLE $table IF NOT EXISTS AS " + data
      case (_, _, _, Some(table)) => s"INSERT INTO $table " + data
      case _                      => data
    }
    logger.info(sql)

    val conn = getConnection(
      config.getStringOrExported("host"),
      config.getStringOrExported("user"),
      this.context.getSecrets.getSecret("snow.password"),
      config.getOptionStringOrExported("database"),
      config.getOptionStringOrExported("schema"),
      config.getOptionStringOrExported("warehouse"),
      config.getOptionStringOrExported("role"),
      (
        config.getOptionStringOrExported("session_unixtime_sql_variable_name"),
        config.getStringOrExported("session_unixtime")
      )
    )
    val stmt = conn.createStatement()
    try {
      val queryTag = getConfigFromOperatorParameterOrExportedParameterOptional(config, "query_tag")
      queryTag.foreach(tag => stmt.execute(s"alter session set QUERY_TAG = $tag"))

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
      unixtimeSetting: (Option[String], String)
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
    unixtimeSetting._1.foreach(x => prop.put("$" + x, unixtimeSetting._2))
    //    logger.debug(prop.toString)
    try {
      DriverManager.getConnection(s"jdbc:snowflake://${host}", prop)
    } catch {
      case e: Throwable =>
        throw new TaskExecutionException(e)
    }
  }
}

// オペレータを生成するファクトリクラス
class SnowOperatorFactory(
    templateEngine: TemplateEngine
) extends OperatorFactory {
  // ↓ これがオペレータの名前になる
  override def getType: String = "snow"

  override def newOperator(context: OperatorContext): Operator = new SnowOperator(context, templateEngine)
}
