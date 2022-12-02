package com.dianchu.py4j;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.kitesdk.data.spi.JsonUtil;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataType;
import py4j.GatewayServer;


public class JsonAvroTool {

    public String avroToSql(String schemaStr) {
        Schema.Parser schemaParser = new Schema.Parser();
        Schema schema = schemaParser.parse(schemaStr);
        DataType schemaDataType = SchemaConverters.toSqlType(schema).dataType();
        StructType SchemaType = (StructType) schemaDataType;
        return SchemaType.toDDL();
    }

    public String avroFromSql(String eventName, String ddl) {
        StructType structType = StructType.fromDDL(ddl);
        Schema schema = SchemaConverters.toAvroType(structType, true, eventName, "com.dianchu");
        return schema.toString();
    }

    public String jsonToAvro(String data, String schemaName) {
        JsonNode dataJson = JsonUtil.parse(data);
        return JsonUtil.inferSchema(dataJson, schemaName).toString();
    }

    public static void main(String[] args) {
        JsonAvroTool app = new JsonAvroTool();
        // app is now the gateway.entry_point
        GatewayServer server = new GatewayServer(app);
        server.start();
    }
}
