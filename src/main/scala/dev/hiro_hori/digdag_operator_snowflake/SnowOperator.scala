package dev.hiro_hori.digdag_operator_snowflake

import collection.JavaConverters._
import io.digdag.spi.{Operator, OperatorContext, OperatorFactory, TaskResult}
import io.digdag.util.BaseOperator
import org.slf4j.LoggerFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Optional
import io.digdag.client.config.Config
import net.snowflake.client.jdbc.SnowflakeDriver

import java.util.Properties

//import java.net.URL
import java.sql.{Connection, DriverManager}
import scala.io.Source
import scala.jdk.OptionConverters._


// オペレータ本体
class SnowOperator(_context: OperatorContext) extends BaseOperator(_context) {
  private[this] val logger = LoggerFactory.getLogger(classOf[SnowOperator])

  override def runTask(): TaskResult = {
    val config = this.request.getConfig

    // Configの内容をJSONのPrettyPrintで出力
    val pretty = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(config)
    logger.debug(pretty)

    val source = Source.fromFile(config.get("_command", classOf[String]))
    val sql = source.mkString
    source.close()
    logger.info(sql)

    val conn = getConnection(
      getConfigFromOperatorParameterOrExportedParameter(config,"host"),
      getConfigFromOperatorParameterOrExportedParameter(config,"user"),
      this.context.getSecrets.getSecret("snow.password"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "db"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "schema"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "warehouse"),
      getConfigFromOperatorParameterOrExportedParameterOptional(config, "role"),
    )
    val stmt = conn.createStatement()
    stmt.execute(sql)
    conn.close()
    // オペレータの処理が無事成功した場合はTaskResultを返す
    TaskResult.empty(this.request)
  }

  def getConnection(
                     host: String,
                     user: String,
                     password: String,
                     db: Option[String],
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
    db.foreach(x => prop.put("db", x))
    schema.foreach(x => prop.put("schema", x))
    warehouse.foreach(x => prop.put("warehouse", x))
    role.foreach(x => prop.put("role", x))
    DriverManager.getConnection(s"jdbc:snowflake://${host}", prop)
  }

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