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

package io.kyligence.kap.storage.parquet.cube.spark.rpc;

import org.apache.spark.SparkConf;

import io.grpc.stub.StreamObserver;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.ConfServiceGrpc;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkConfRequest;
import io.kyligence.kap.storage.parquet.cube.spark.rpc.generated.SparkJobProtos.SparkConfResponse;

public class SparkConfService extends ConfServiceGrpc.ConfServiceImplBase {

    @Override
    public void getConf(SparkConfRequest sparkConfRequest, StreamObserver<SparkConfResponse> streamObserver) {
        SparkConf sparkConf = new SparkConf();
        String value = sparkConf.get(sparkConfRequest.getName());
        SparkConfResponse response = SparkConfResponse.newBuilder().setValue(value).build();
        streamObserver.onNext(response);
        streamObserver.onCompleted();
    }
}
