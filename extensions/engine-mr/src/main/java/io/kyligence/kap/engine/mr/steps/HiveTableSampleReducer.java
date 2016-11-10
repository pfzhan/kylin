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

package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.engine.mr.KylinReducer;

public class HiveTableSampleReducer extends KylinReducer<IntWritable, Text, IntWritable, Text> {

    public static final int ONE = 1;
    private Map<Integer, HiveSampler> sampleMap = new HashMap<Integer, HiveSampler>();
    protected Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int skey = key.get();
        for (Text v : values) {
            ByteBuffer buffer = ByteBuffer.wrap(v.getBytes());
            System.out.println("-----------------value length:   " + buffer.limit());
            HiveSampler sampler = new HiveSampler();
            sampler.decode(buffer);
            if (!sampleMap.containsKey(skey))
                sampleMap.put(skey, new HiveSampler());
            else {
                sampleMap.get(skey);
            }
            sampleMap.get(skey).merge(sampler);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Integer> keys = new ArrayList<Integer>();
        Iterator<Integer> it = sampleMap.keySet().iterator();
        while (it.hasNext()) {
            keys.add(it.next());
        }
        Collections.sort(keys);
        it = keys.iterator();
        while (it.hasNext()) {
            int key = it.next();
            HiveSampler sampler = sampleMap.get(key);
            sampler.code();
            outputValue.set(sampler.getBuffer().array(), 0, sampler.getBuffer().position());
            context.write(new IntWritable(key), outputValue);
        }
    }
}
