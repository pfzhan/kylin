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
package io.kyligence.kap.tool.bisync.model;

import org.apache.kylin.metadata.model.JoinTableDesc;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class JoinTreeNode {

    private JoinTableDesc value;

    private List<JoinTreeNode> childNodes;

    public JoinTableDesc getValue() {
        return value;
    }

    public void setValue(JoinTableDesc value) {
        this.value = value;
    }

    public List<JoinTreeNode> getChildNodes() {
        return childNodes;
    }

    public void setChildNodes(List<JoinTreeNode> childNodes) {
        this.childNodes = childNodes;
    }

    /**
     * serialize a tree node to list by level-first
     */
    public List<JoinTableDesc> iteratorAsList() {
        if (this.value == null) {
            return null;
        } else {
            Deque<JoinTreeNode> nodeDeque = new LinkedList<>();
            List<JoinTableDesc> elements = new LinkedList<>();
            nodeDeque.push(this);
            breadthSerialize(nodeDeque, elements);
            return elements;
        }
    }

    private void breadthSerialize(Deque<JoinTreeNode> nodeDeque, List<JoinTableDesc> elements) {
        if (nodeDeque.size() == 0) {
            return;
        } else {
            JoinTreeNode node = nodeDeque.removeFirst();
            elements.add(node.getValue());
            if (node.getChildNodes() != null) {
                for (JoinTreeNode childNode : node.getChildNodes()) {
                    nodeDeque.addLast(childNode);
                }
            }
            breadthSerialize(nodeDeque, elements);
        }
    }
}
