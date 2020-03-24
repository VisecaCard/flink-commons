#set($dollar = "$")
package ${package}.components

import org.apache.flink.api.common.functions.MapFunction

class StringWrapper extends MapFunction[String, String] {

  var prefix: String = ""

  def setPrefix(prefix: String) = this.prefix = prefix

  var postfix: String = ""

  def setPostfix(postfix: String) = this.postfix = postfix

  override def map(text: String): String = {
    s"${dollar}prefix${dollar}text${dollar}postfix"
  }
}
