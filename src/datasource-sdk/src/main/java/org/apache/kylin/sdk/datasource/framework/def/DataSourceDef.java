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
package org.apache.kylin.sdk.datasource.framework.def;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@JacksonXmlRootElement(localName = "DATASOURCE_DEF")
public class DataSourceDef {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceDef.class);

    @JacksonXmlProperty(localName = "NAME", isAttribute = true)
    private String name;
    @JacksonXmlProperty(localName = "ID", isAttribute = true)
    private String id;
    @JacksonXmlProperty(localName = "DIALECT", isAttribute = true)
    private String dialect;

    @JacksonXmlProperty(localName = "PROPERTY")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<PropertyDef> properties;

    @JacksonXmlProperty(localName = "FUNCTION_DEF")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<FunctionDef> functions;

    @JacksonXmlProperty(localName = "TYPE_DEF")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<TypeDef> types;

    @JacksonXmlProperty(localName = "TYPE_MAPPING")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<TypeMapping> typeMappings;

    // calculated members
    private Map<String, SqlNode> functionDefSqlNodeMap; // defId <---> SqlCall or SqlIdentifier
    private Map<String, List<String>> functionNameDefMap; // name <---> defId[], because multiple function_def may share same name.
    private Map<String, TypeDef> typeDefMap; // id <---> TypeDef
    private Map<String, List<TypeDef>> typeNameDefMap; // Name <---> TypeDef
    private Map<String, PropertyDef> propertyDefMap;
    private Map<String, Integer> dataTypeMap;
    private static final Map<String, Integer> TYPE_VALUES_MAP;

    static {
        Class clazz = java.sql.Types.class;
        Field[] fileds = clazz.getDeclaredFields();
        TYPE_VALUES_MAP = new HashMap<>(fileds.length);

        for (Field field : fileds) {
            try {
                TYPE_VALUES_MAP.put(field.getName(), field.getInt(clazz));
            } catch (IllegalAccessException e) {
                logger.error("failed to load java.sql.Types.", e);
            }
        }
    }

    public DataSourceDef() {
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public void init() {
        functionDefSqlNodeMap = Maps.newHashMap();
        functionNameDefMap = Maps.newHashMap();
        typeNameDefMap = Maps.newHashMap();
        typeDefMap = Maps.newHashMap();
        propertyDefMap = Maps.newHashMap();
        dataTypeMap = Maps.newHashMap();

        if (functions != null) {
            for (FunctionDef function : functions) {
                function.init();
                try {
                    SqlParser sqlParser = SqlParser.create(function.getExpression());
                    SqlNode parsed = sqlParser.parseExpression();
                    if (parsed instanceof SqlCall || parsed instanceof SqlIdentifier) {
                        String name = parsed instanceof SqlCall ? ((SqlCall) parsed).getOperator().getName()
                                : parsed.toString();
                        List<String> defIds = functionNameDefMap.get(name);
                        if (defIds == null) {
                            defIds = Lists.newLinkedList();
                            functionNameDefMap.put(name, defIds);
                        }
                        defIds.add(function.getId());
                        functionNameDefMap.put(name, defIds);
                        functionDefSqlNodeMap.put(function.getId(), parsed);
                    } else {
                        throw new IllegalStateException("Not a valid SqlCall.");
                    }
                } catch (Throwable e) {
                    logger.error("Failed to load function: ID={}, EXPRESSION={}", function.getId(),
                            function.getExpression(), e);
                }
            }
        }

        if (types != null) {
            for (TypeDef type : types) {
                try {
                    type.init();
                    List<TypeDef> defs = typeNameDefMap.get(type.getName());
                    if (defs == null) {
                        defs = Lists.newLinkedList();
                        typeNameDefMap.put(type.getName(), defs);
                    }
                    defs.add(type);
                    typeDefMap.put(type.getId(), type);
                } catch (Throwable e) {
                    logger.error("Failed to load type: ID={}, NAME={}, EXPRESSION={}", type.getId(), type.getName(),
                            type.getExpression());
                }
            }
        }

        if (properties != null) {
            for (PropertyDef prop : properties) {
                propertyDefMap.put(prop.getName().toLowerCase(Locale.ROOT), prop);
            }
        }

        if (typeMappings != null) {
            for (TypeMapping typeMapping : typeMappings) {
                String sourceType = typeMapping.getSourceType();
                String targetType = typeMapping.getTargetType();
                Integer typeValue = TYPE_VALUES_MAP.get(targetType.toUpperCase(Locale.ROOT));
                if (typeValue == null) {
                    logger.error("target dataType can not be found in java.sql.Types, SOURCE_TYPE={}, TARGET_TYPE={}",
                            sourceType, targetType);
                } else {
                    dataTypeMap.put(sourceType.toUpperCase(Locale.ROOT), typeValue);
                }
            }
        }
    }

    // ===================================================================

    public SqlNode getFuncDefSqlNode(String id) {
        return functionDefSqlNodeMap.get(id.toUpperCase(Locale.ROOT));
    }

    public List<String> getFuncDefsByName(String name) {
        return functionNameDefMap.get(name.toUpperCase(Locale.ROOT));
    }

    public TypeDef getTypeDef(String id) {
        return typeDefMap.get(id.toUpperCase(Locale.ROOT));
    }

    public List<TypeDef> getTypeDefsByName(String typeName) {
        return typeNameDefMap.get(typeName.toUpperCase(Locale.ROOT));
    }

    public String getPropertyValue(String name) {
        return getPropertyValue(name, null);
    }

    public String getPropertyValue(String name, String defaultValue) {
        if (name == null)
            return defaultValue;

        PropertyDef prop = propertyDefMap.get(name.toLowerCase(Locale.ROOT));
        return prop == null ? defaultValue : prop.getValue();
    }

    public String getValidationQuery() {
        return getPropertyValue("source.validation-query");
    }

    public Integer getDataTypeValue(String sourceType) {
        return dataTypeMap.get(sourceType.toUpperCase(Locale.ROOT));
    }

    public Map<String, TypeDef> getTypeDefMap() {
        return this.typeDefMap;
    }

    public String getDialectName() {
        return this.dialect;
    }
}
