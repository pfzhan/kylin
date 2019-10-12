/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.metadata.recommendation;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Topo<T> {
    private List<Graph.Node<T>> result;
    private Queue<Graph.Node<T>> setOfZeroInDegree;
    private Graph<T> graph;

    public Topo(Graph<T> di) {
        this.graph = di;
        this.result = new ArrayList<>();
        this.setOfZeroInDegree = new LinkedList<>();
        for (Graph.Node<T> iterator : this.graph.vertexSet) {
            if (iterator.pathIn == 0) {
                this.setOfZeroInDegree.add(iterator);
            }
        }
        this.process();
    }

    private void process() {
        while (!setOfZeroInDegree.isEmpty()) {
            Graph.Node v = setOfZeroInDegree.poll();

            result.add(v);

            if (this.graph.outNode.keySet().isEmpty()) {
                return;
            }

            for (Graph.Node<T> w : this.graph.outNode.get(v)) {
                w.pathIn--;
                if (0 == w.pathIn) {
                    setOfZeroInDegree.add(w);
                }
            }
            this.graph.vertexSet.remove(v);
            this.graph.outNode.remove(v);
        }

        if (!this.graph.vertexSet.isEmpty()) {
            throw new DependencyLostException("CC dependency contains cycle");
        }
    }

    public Iterable<Graph.Node<T>> getResult() {
        return result;
    }

}
