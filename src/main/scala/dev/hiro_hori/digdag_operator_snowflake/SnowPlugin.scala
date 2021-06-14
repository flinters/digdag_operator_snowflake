package dev.hiro_hori.digdag_operator_snowflake

import java.util
import io.digdag.spi.{OperatorFactory, OperatorProvider, Plugin}
import javax.inject.Inject
class SnowPlugin extends Plugin {
  override def getServiceProvider[T](`type`: Class[T]): Class[_ <: T] =
    if (`type` eq classOf[OperatorProvider]) classOf[SnowOperatorProvider].asSubclass(`type`) else null
}

class SnowOperatorProvider @Inject() extends OperatorProvider {
  override def get(): util.List[OperatorFactory] = util.Arrays.asList(new SnowOperatorFactory)
}
