package com.pwc.dataflow.example.normalizedItem.product;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
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

        // Grab all relevant fields
        String endpointType = propMap.get("endpoint_type").getStringValue();
        String excludeFromIndexes = propMap.get("exclude_from_indexes")==null?"":propMap.get("exclude_from_indexes").getStringValue();
        String updatedAt = propMap.get("updated_at").getTimestampValue().toString();
        String provider = propMap.get("provider").getStringValue();
        String changeset = String.valueOf(propMap.get("changeset").getIntegerValue());
        String itemType = propMap.get("item_type").getStringValue();
        String endpointId = propMap.get("endpoint_id").getStringValue();

        String data = propMap.get("data").getBlobValue().toStringUtf8();
        JSONObject jsonObject = (JSONObject)(new JSONParser().parse(data));

        String dataId = jsonObject.get("id")==null?"":jsonObject.get("id").toString();
        String dataOrgId = jsonObject.get("orgId").toString();
        String dataCreatedAt = jsonObject.get("createdAt").toString();
        String dataUpdatedAt = jsonObject.get("updatedAt").toString();
        String dataDescription = jsonObject.get("description").toString();
        String dataReference = jsonObject.get("reference")==null?"":jsonObject.get("reference").toString();
        String dataIsSold = jsonObject.get("isSold").toString();
        String dataIsPurchased = jsonObject.get("isPurchased").toString();

        String jsonString = "{\"provider\":\""+ provider +"\",\"updatedAt\":\""+updatedAt +"\",\"itemType\":\""+itemType
                +"\",\"endpointId\":\""+endpointId+"\",\"endpointType\":\""+ endpointType
                +"\",\"excludeFromIndexes\":\""+excludeFromIndexes+"\",\"changeset\":\""+changeset
                +"\",\"dataId\":\""+dataId+"\",\"dataOrgId\":\""+dataOrgId+"\",\"dataCreatedAt\":\""+dataCreatedAt
                +"\",\"dataUpdatedAt\":\""+dataUpdatedAt+"\",\"dataDescription\":\""+dataDescription+"\",\"dataReference\":\""+dataReference
                +"\",\"dataIsSold\":\""+dataIsSold+"\",\"dataIsPurchased\":\""+dataIsPurchased+"\"}";

        out.output(jsonString);

    }

}
