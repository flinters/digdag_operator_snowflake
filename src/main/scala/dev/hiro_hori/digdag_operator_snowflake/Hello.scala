package dev.hiro_hori.digdag_operator_snowflake

object Hello extends Greeting with App {
  println(greeting)
}

trait Greeting {
  lazy val greeting = "hello"
}
