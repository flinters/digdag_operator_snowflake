package dev.hiro_hori.digdag_operator_snowflake

import io.digdag.spi.{Operator, OperatorContext, OperatorFactory, TaskResult}
import io.digdag.util.BaseOperator
import org.slf4j.LoggerFactory

// オペレータ本体
class ExampleOperator(_context: OperatorContext) extends BaseOperator(_context) {
  private[this] val logger = LoggerFactory.getLogger(classOf[ExampleOperator])

  override def runTask(): TaskResult = {
    logger.info("Hello, Digdag!")
    // オペレータの処理が無事成功した場合はTaskResultを返す
    TaskResult.empty(this.request)
  }
}

// オペレータを生成するファクトリクラス
class ExampleOperatorFactory extends OperatorFactory {
  // ↓ これがオペレータの名前になる
  override def getType: String = "example"

  override def newOperator(context: OperatorContext): Operator =
    new ExampleOperator(context)
}