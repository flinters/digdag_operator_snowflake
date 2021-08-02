package dev.hiro_hori.digdag_operator_snowflake

import io.digdag.client.config.Config

extension (config: Config) {

  def exported: Config = config.getNested("snow")

  def getString(name: String): String = config.get(name, classOf[String])

  def getOptionString(name: String): Option[String] = Option(config.getOptional(name, classOf[String]).orNull())

  def getStringOrExported(name: String): String = getOptionString(name).getOrElse(exported.getString(name))

  def getOptionStringOrExported(name: String): Option[String] =
    getOptionString(name).orElse(exported.getOptionString(name))
}
