package com.pwc.dataflow.example.normalizedItem.account;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class EntityToString extends DoFn<Entity, String> {
    private static final Logger LOG = LoggerFactory.getLogger(com.pwc.dataflow.example.EntityToString.class);
    @ProcessElement
    public void processElement(@Element Entity dataStore, OutputReceiver<String> out)
            throws IOException, ParseException {

        Map<String, Value> propMap = dataStore.getPropertiesMap();
        JSONObject result = new JSONObject();
        for (Map.Entry<String, Value> entry : propMap.entrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            Value.ValueTypeCase type = value.getValueTypeCase();
            switch(type.toString()){
                case "TIMESTAMP_VALUE":
                    com.google.protobuf.Timestamp timeValue = value.getTimestampValue();
                    result.put(key, timeValue.toString());
                    break;
                case "STRING_VALUE":
                    String strValue = value.getStringValue();
                    result.put(key, strValue);
                    break;
                case "INTEGER_VALUE":
                    Long intValue = value.getIntegerValue();
                    result.put(key, intValue);
                    break;
                case "BLOB_VALUE":
                    ByteString blobString = value.getBlobValue();
                    JSONObject jsonObject = (JSONObject)(new JSONParser().parse(blobString.toStringUtf8()));
                    for(Object obKey: jsonObject.keySet()){
                        String keyStr = (String)obKey;
                        Object keyValue = jsonObject.get(keyStr);
                        result.put(keyStr.replace(".","_"), keyValue);
                    }
                    break;
                default:
                    break;
            }
        }
        out.output(result.toJSONString());

    }

}
