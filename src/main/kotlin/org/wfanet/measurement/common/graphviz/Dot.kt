// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common.graphviz

/** Builds a DOT-language digraph. */
fun digraph(name: String? = null, init: Graph.() -> Unit): String {
  val graph = PrintableGraph("digraph${name.withSpaceBefore()}").apply(init)
  return Printer().also(graph::render).toString()
}

@DslMarker annotation class GraphvizDsl

@GraphvizDsl interface Element

interface Attributes : Element {
  fun set(pair: Pair<String, String>)
}

interface Scope : Element {
  fun node(name: String, initAttributes: Attributes.() -> Unit = {})
  fun edge(pair: Pair<String, String>, initAttributes: Attributes.() -> Unit = {})
  fun scope(name: String? = null, init: Scope.() -> Unit)
  fun attributes(init: Attributes.() -> Unit)
}

interface Graph : Scope {
  fun subgraph(name: String? = null, init: Graph.() -> Unit)
}

@GraphvizDsl
private interface Printable {
  fun render(printer: Printer)
}

private class Printer {
  private var indent: Int = 0
  private val buffer = StringBuffer()

  fun outputLine(string: String) {
    outputIndent()
    buffer.appendLine(string)
  }

  fun output(string: String) {
    buffer.append(string)
  }

  fun outputIndent() {
    repeat(indent) { buffer.append("  ") }
  }

  fun endLine() {
    output("\n")
  }

  fun withIndent(block: () -> Unit) {
    indent++
    block()
    indent--
  }

  override fun toString(): String {
    return buffer.toString()
  }
}

private abstract class GenericAttributes : Attributes, Printable {
  protected val attributes = mutableMapOf<String, String>()

  final override fun set(pair: Pair<String, String>) {
    attributes[pair.first] = pair.second
  }
}

private class CommaSeparatedAttributes : GenericAttributes() {
  override fun render(printer: Printer) {
    if (attributes.isEmpty()) return
    val string =
      attributes.toList().joinToString(", ", prefix = " [", postfix = "]") { (k, v) -> "$k=\"$v\"" }
    printer.output(string)
  }
}

private class NewlineAttributes : GenericAttributes() {
  override fun render(printer: Printer) {
    for ((key, value) in attributes) {
      printer.outputLine("$key=\"$value\"")
    }
  }
}

private open class PrintableScope(private val name: String?) : Scope, Printable {
  private val children = mutableListOf<Printable>()
  private val attributes = NewlineAttributes()

  override fun render(printer: Printer) {
    with(printer) {
      outputLine("${name.withSpaceAfter()}{")
      withIndent {
        attributes.render(this)
        for (child in children) child.render(this)
      }
      outputLine("}")
    }
  }

  protected fun addChild(child: Printable) {
    children.add(child)
  }

  override fun node(name: String, initAttributes: Attributes.() -> Unit) {
    addChild(Node(name, CommaSeparatedAttributes().apply(initAttributes)))
  }

  override fun edge(pair: Pair<String, String>, initAttributes: Attributes.() -> Unit) {
    addChild(Edge(pair.first, pair.second, CommaSeparatedAttributes().apply(initAttributes)))
  }

  override fun scope(name: String?, init: Scope.() -> Unit) {
    addChild(PrintableScope(name).apply(init))
  }

  override fun attributes(init: Attributes.() -> Unit) {
    attributes.apply(init)
  }
}

@GraphvizDsl
private class PrintableGraph(name: String) : Graph, PrintableScope(name) {
  override fun subgraph(name: String?, init: Graph.() -> Unit) {
    addChild(PrintableGraph("subgraph${name.withSpaceBefore()}").apply(init))
  }
}

@GraphvizDsl
private abstract class EdgeOrNode(private val attributes: CommaSeparatedAttributes) : Printable {
  abstract val body: String

  override fun render(printer: Printer) {
    printer.outputIndent()
    printer.output(body)
    attributes.render(printer)
    printer.endLine()
  }
}

@GraphvizDsl
private class Edge(from: String, to: String, attributes: CommaSeparatedAttributes) :
  EdgeOrNode(attributes) {
  override val body: String = "$from -> $to"
}

@GraphvizDsl
private class Node(override val body: String, attributes: CommaSeparatedAttributes) :
  EdgeOrNode(attributes)

private fun String?.withSpaceBefore(): String {
  return this?.let { " $this" } ?: ""
}

private fun String?.withSpaceAfter(): String {
  return this?.let { "$this " } ?: ""
}
