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

package io.kyligence.kap.metadata.filter;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.DynamicTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.datatype.OrderedBytesStringSerializer;

/**
 * EvaluatableFunctionTupleFilter is a hacky way to support better performance
 * like function evaluation in Raw Table
 */
public class EvaluatableFunctionTupleFilter extends BuiltInFunctionTupleFilter {

    private boolean constantsInitted = false;

    //about non-like
    private List<Object> values;
    private Object tupleValue;

    public EvaluatableFunctionTupleFilter(String name) {
        super(name, FilterOperatorEnum.EVAL_FUNC);
        values = Lists.newArrayListWithCapacity(1);
        values.add(null);
    }

    //used by refactor in TupleFilterSerializer
    @SuppressWarnings("unused")
    public EvaluatableFunctionTupleFilter() {
        super(null, FilterOperatorEnum.EVAL_FUNC);
        values = Lists.newArrayListWithCapacity(1);
        values.add(null);
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem cs) {

        // extract tuple value
        Object tupleValue = null;
        for (TupleFilter filter : this.children) {
            if (!isConstant(filter)) {
                filter.evaluate(tuple, cs);
                tupleValue = filter.getValues().iterator().next();
            }
        }

        TblColRef tblColRef = this.getColumn();
        DataType strDataType = DataType.getType("string");
        if (tblColRef.getType() != strDataType) {
            throw new IllegalStateException("Only String type is allow in BuiltInFunction");
        }
        ByteArray valueByteArray = (ByteArray) tupleValue;
        OrderedBytesStringSerializer serializer = new OrderedBytesStringSerializer(strDataType);
        String value = serializer.deserialize(ByteBuffer.wrap(valueByteArray.array(), valueByteArray.offset(), valueByteArray.length()));

        try {
            if (isLikeFunction()) {
                return (Boolean) invokeFunction(value);
            } else {
                this.tupleValue = invokeFunction(value);
                //convert back to ByteArray format because the outer EvaluatableFunctionTupleFilter assumes input as ByteArray
                ByteBuffer buffer = ByteBuffer.allocate(valueByteArray.length() * 2);
                serializer.serialize((String) this.tupleValue, buffer);
                this.tupleValue = new ByteArray(buffer.array(), 0, buffer.position());

                return true;
            }
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<?> getValues() {
        this.values.set(0, tupleValue);
        return values;
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        if (!isValid()) {
            throw new IllegalStateException("must be valid");
        }
        BytesUtil.writeUTFString(name, buffer);
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        this.name = BytesUtil.readUTFString(buffer);
        this.initMethod();
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    private boolean isConstant(TupleFilter filter) {
        return (filter instanceof ConstantTupleFilter) || (filter instanceof DynamicTupleFilter);
    }

    @Override
    public Object invokeFunction(Object input) throws InvocationTargetException, IllegalAccessException {
        if (isLikeFunction())
            initConstants();
        return super.invokeFunction(input);
    }

    private void initConstants() {
        if (constantsInitted) {
            return;
        }
        //will replace the ByteArray pattern to String type
        ByteArray byteArray = (ByteArray) methodParams.get(constantPosition);
        OrderedBytesStringSerializer s = new OrderedBytesStringSerializer(DataType.getType("string"));
        String pattern = s.deserialize(ByteBuffer.wrap(byteArray.array(), byteArray.offset(), byteArray.length()));
        //TODO 
        //pattern = pattern.toLowerCase();//to remove upper case
        methodParams.set(constantPosition, pattern);
        constantsInitted = true;
    }

    //even for "tolower(s)/toupper(s)/substring(like) like pattern", the like pattern can be used for index searching
    public String getLikePattern() {
        if (!isLikeFunction()) {
            return null;
        }

        initConstants();
        return (String) methodParams.get(1);
    }

    public boolean isLikeFunction() {
        return "like".equalsIgnoreCase(this.getName());
    }

}
