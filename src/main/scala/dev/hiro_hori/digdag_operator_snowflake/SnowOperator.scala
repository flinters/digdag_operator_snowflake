package dev.hiro_hori.digdag_operator_snowflake

import collection.JavaConverters._
import io.digdag.spi.{Operator, OperatorContext, OperatorFactory, TaskResult}
import io.digdag.util.BaseOperator
import org.slf4j.LoggerFactory
import com.fasterxml.jackson.databind.ObjectMapper

import scala.io.Source


// オペレータ本体
class SnowOperator(_context: OperatorContext) extends BaseOperator(_context) {
  private[this] val logger = LoggerFactory.getLogger(classOf[SnowOperator])

  override def runTask(): TaskResult = {
    val config = this.request.getConfig

    // Configの内容をJSONのPrettyPrintで出力
    val pretty = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(config)
    logger.info(pretty)
    logger.info("key = {}", config.get("key", classOf[String]))
    logger.info("array = {}", config.getList("array", classOf[Int]).asScala.toSeq)

    val source = Source.fromFile(config.get("_command", classOf[String]))
    val sql = source.mkString
    source.close()
    logger.info(sql)
    // オペレータの処理が無事成功した場合はTaskResultを返す
    TaskResult.empty(this.request)
  }
}

// オペレータを生成するファクトリクラス
class SnowOperatorFactory extends OperatorFactory {
  // ↓ これがオペレータの名前になる
  override def getType: String = "snow"

  override def newOperator(context: OperatorContext): Operator =
    new SnowOperator(context)
}