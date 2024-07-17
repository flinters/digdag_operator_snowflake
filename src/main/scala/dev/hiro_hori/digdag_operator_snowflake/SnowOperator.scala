package dev.hiro_hori.digdag_operator_snowflake

import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigKey}
import io.digdag.spi._
import io.digdag.util.BaseOperator
import net.snowflake.client.jdbc.{SnowflakeDriver, SnowflakeResultSet, SnowflakeStatement}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.reflect.ClassTag


// オペレータ本体
class SnowOperator(_context: OperatorContext, templateEngine: TemplateEngine) extends BaseOperator(_context) {
  private[this] val logger = LoggerFactory.getLogger(classOf[SnowOperator])

  private[this] def classOfT[T: ClassTag]: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  private[this] case class QueryResult(id: String)

  override def runTask(): TaskResult = {
    val localConfig = request.getLocalConfig
    val exportedConfig = request.getConfig.getNested("snow")

    val data = try {
      workspace.templateCommand(templateEngine, request.getConfig, "query", UTF_8)
    } catch {
      case e: Throwable => throw new TaskExecutionException(e)
    }

    val createTable = getOptionalParameterFromOperatorParameter[String](localConfig, "create_table")
    val createOrReplaceTable = getOptionalParameterFromOperatorParameter[String](localConfig, "create_or_replace_table")
    val createTableIfNotExists = getOptionalParameterFromOperatorParameter[String](localConfig, "create_table_if_not_exists")
    val insertInto = getOptionalParameterFromOperatorParameter[String](localConfig, "insert_into")
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
      getConfigFromOperatorParameterOrExportedParameter[String](localConfig, exportedConfig, "host"),
      getConfigFromOperatorParameterOrExportedParameter[String](localConfig, exportedConfig, "user"),
      context.getSecrets.getSecret("snow.password"),
      getConfigFromOperatorParameterOrExportedParameterOptional[String](localConfig, exportedConfig, "database"),
      getConfigFromOperatorParameterOrExportedParameterOptional[String](localConfig, exportedConfig, "schema"),
      getConfigFromOperatorParameterOrExportedParameterOptional[String](localConfig, exportedConfig, "warehouse"),
      getConfigFromOperatorParameterOrExportedParameterOptional[String](localConfig, exportedConfig, "role"),
      getConfigFromOperatorParameterOrExportedParameterOptional[String](localConfig, exportedConfig, "query_tag"),
      getConfigFromOperatorParameterOrExportedParameterOptional[String](localConfig, exportedConfig, "timezone"),
      (
        getConfigFromOperatorParameterOrExportedParameterOptional[String](localConfig, exportedConfig, "session_unixtime_sql_variable_name"),
        request.getConfig.get("session_unixtime", classOf[String])
      )
    )
    val stmt = conn.createStatement()
    try {
      val multiQueries = getConfigFromOperatorParameterOrExportedParameterOptional[Boolean](localConfig, exportedConfig, "multi_queries").getOrElse(false)
      val storeLastResults = getOptionalParameterFromOperatorParameter[Boolean](localConfig, "store_last_results").getOrElse(false)

      if (multiQueries) {
        stmt.unwrap(classOf[SnowflakeStatement]).setParameter("MULTI_STATEMENT_COUNT", 0)
      }

      stmt.execute(sql)

      // @see https://docs.snowflake.com/en/user-guide/jdbc-using.html#multi-statement-support
      // stmt.getMoreResultsは、CREATE TABLE 文などで false を返します。
      // しかし getMoreResults 以外に statement をイテレーションする手段が見つけられませんでした。
      // そのため実行されたstatement数を正確に数える手段がなく、";"をカウントして代わりとしています。
      // 正確なstatement数がわからないので、loopは可能性のあるstatement数の最大分を回し、さらに重複して抽出したstatementは排除するという考えのもと実装しています。
      val maxStmt = sql.count(_ == ';') + 1
      val queryResults = collection.mutable.Set[QueryResult]()

      var store = request.getConfig.getFactory.create()

      for (_ <- 0 until maxStmt) {
        val result = stmt.getResultSet()
        if (result != null) {
          if (storeLastResults && !result.isClosed) store = buildStoreParam(result)
          queryResults.add(QueryResult(result.unwrap(classOf[SnowflakeResultSet]).getQueryID))
        }
        stmt.getMoreResults
      }
      if (queryResults.isEmpty) { // 全statementにSELECT文が一つもない場合
        queryResults.add(QueryResult(stmt.unwrap(classOf[SnowflakeStatement]).getQueryID))
        if (storeLastResults) store.getNestedOrSetEmpty("snow").getNestedOrSetEmpty("last_results")
      }
      val output = buildOutputParam(sql, queryResults.toList)

      val builder = TaskResult.defaultBuilder(request)
      builder.resetStoreParams(ImmutableList.of(ConfigKey.of("snow", "last_query")))
      if(storeLastResults) {
        builder.resetStoreParams(ImmutableList.of(ConfigKey.of("snow", "last_results")))
      }
      builder.storeParams(output.merge(store))
      builder.build()
    } catch {
      case e: Throwable => throw new TaskExecutionException(e)
    } finally {
      conn.close()
    }
  }

  private def getConnection(
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

  private def getOptionalParameterFromOperatorParameter[T: ClassTag](localConfig: Config, configName: String): Option[T] =
    Option(localConfig.getOptional(configName, classOfT[T]).orNull())

  private def getConfigFromOperatorParameterOrExportedParameter[T: ClassTag](localConfig: Config, exportedConfig: Config, configName: String): T =
    Option(localConfig.getOptional(configName, classOfT[T]).orNull())
      .getOrElse(exportedConfig.get(configName, classOfT[T]))

  private def getConfigFromOperatorParameterOrExportedParameterOptional[T: ClassTag](localConfig: Config, exportedConfig: Config, configName: String): Option[T] = {

    val o0: Option[T] = Option(localConfig.getOptional(configName, classOfT[T]).orNull())
    val o1: Option[T] = Option(exportedConfig.getOptional(configName, classOfT[T]).orNull())
    if (o0.isDefined) {
      o0
    } else {
      o1
    }
  }

  private def buildOutputParam(sql: String, queries: List[QueryResult]): Config = {
    val ret = request.getConfig.getFactory.create()
    val lastQueryParam = ret.getNestedOrSetEmpty("snow").getNestedOrSetEmpty("last_query")

    lastQueryParam.set("ids", java.util.Arrays.asList(queries.map(_.id): _*))
    lastQueryParam.set("query", sql)
    ret
  }

  private def buildStoreParam(rs: ResultSet): Config = {
    val ret = request.getConfig.getFactory.create()
    val map = new java.util.LinkedHashMap[String, AnyRef]()
    val metadata = rs.getMetaData

    if(rs.next()) {
      (1 to metadata.getColumnCount).foreach { i =>
        map.put(metadata.getColumnName(i), rs.getObject(i))
      }
    }

    ret.getNestedOrSetEmpty("snow").set("last_results", map)
    ret
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