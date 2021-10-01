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

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DotTest {
  @Test
  fun renderGraph() {
    val graph =
      digraph("Foo") {
        attributes { set("splines" to "ortho") }
        subgraph {
          edge("A" to "B")
          edge("B" to "C") { set("label" to "some-label") }
        }

        scope {
          attributes { set("rank" to "same") }
          node("X")
          node("Y") { set("foo" to "bar") }
        }
        edge("X" to "Y")
      }

    val expected =
      """
      digraph Foo {
        splines="ortho"
        subgraph {
          A -> B
          B -> C [label="some-label"]
        }
        {
          rank="same"
          X
          Y [foo="bar"]
        }
        X -> Y
      }
      """.trimIndent()

    assertThat(graph.trim()).isEqualTo(expected)
  }
}
