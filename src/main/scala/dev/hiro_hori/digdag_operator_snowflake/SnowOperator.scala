package dev.hiro_hori.digdag_operator_snowflake

import io.digdag.spi.{Operator, OperatorContext, OperatorFactory, TaskResult}
import io.digdag.util.BaseOperator
import org.slf4j.LoggerFactory
import com.fasterxml.jackson.databind.ObjectMapper
import io.digdag.client.config.Config
import net.snowflake.client.jdbc.{SnowflakeDriver, SnowflakeSQLException}

import java.util.Properties
import java.sql.{Connection, DriverManager}
import scala.io.Source


// オペレータ本体
class SnowOperator(_context: OperatorContext) extends BaseOperator(_context) {
  private[this] val logger = LoggerFactory.getLogger(classOf[SnowOperator])

  override def runTask(): TaskResult = {
    val config = this.request.getConfig

    // Configの内容をJSONのPrettyPrintで出力
    val pretty = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(config)
    logger.debug(pretty)

    val source = Source.fromFile(config.get("_command", classOf[String]))
    val createTable = getOptionalParameterFromOperatorParameter(config, "create_table")
    val createOrReplaceTable = getOptionalParameterFromOperatorParameter(config, "create_or_replace_table")
    val createTableIfNotExists = getOptionalParameterFromOperatorParameter(config, "create_table_if_not_exists")
    val insertInto = getOptionalParameterFromOperatorParameter(config, "insert_into")
    if (Seq(createTable, createOrReplaceTable, createTableIfNotExists, insertInto).count(_.isDefined) >= 2) {
      throw new RuntimeException("you must specify only 1 option in (create_table, create_or_replace_table, create_table_if_not_exists, insert_into)")
    }
    val sql = (
      getOptionalParameterFromOperatorParameter(config, "create_table"),
      getOptionalParameterFromOperatorParameter(config, "create_or_replace_table"),
      getOptionalParameterFromOperatorParameter(config, "create_table_if_not_exists"),
      getOptionalParameterFromOperatorParameter(config, "insert_into")
    ) match {
      case (Some(table), _, _, _) => s"CREATE TABLE $table AS " + source.mkString
      case (_, Some(table), _, _) => s"CREATE OR REPLACE TABLE $table AS " + source.mkString
      case (_, _, Some(table), _) => s"CREATE TABLE $table IF NOT EXISTS AS " + source.mkString
      case (_, _, _, Some(table)) => s"INSERT INTO $table " + source.mkString
      case _ => source.mkString
    }
    source.close()
    logger.info(sql)

    val conn = getConnection(
      getConfigFromOperatorParameterOrExportedParameter(config,"host"),
      getConfigFromOperatorParameterOrExportedParameter(config,"user"),
      this.context.getSecrets.getSecret("snow.password"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "database"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "schema"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "warehouse"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "role"),
    )
    val stmt = conn.createStatement()
    try {
      stmt.execute(sql)
    } finally {
      conn.close()
    }
    // オペレータの処理が無事成功した場合はTaskResultを返す
    TaskResult.empty(this.request)
  }

  def getConnection(
                     host: String,
                     user: String,
                     password: String,
                     database: Option[String],
                     schema: Option[String],
                     warehouse: Option[String],
                     role: Option[String]
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
//    logger.debug(prop.toString)
    try {
      DriverManager.getConnection(s"jdbc:snowflake://${host}", prop)
    } catch {
      case e: SnowflakeSQLException =>
        throw new RuntimeException(e)
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
class SnowOperatorFactory extends OperatorFactory {
  // ↓ これがオペレータの名前になる
  override def getType: String = "snow"

  override def newOperator(context: OperatorContext): Operator =
    new SnowOperator(context)
}