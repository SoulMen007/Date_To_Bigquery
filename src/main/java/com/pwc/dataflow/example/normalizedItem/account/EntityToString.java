package com.pwc.dataflow.example.normalizedItem.account;

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

        String dataId = jsonObject.get("id").toString();
        String dataOrgId = jsonObject.get("orgId").toString();
        String dataCreatedAt = jsonObject.get("createdAt").toString();
        String dataUpdatedAt = jsonObject.get("updatedAt").toString();
        String dataName = jsonObject.get("name").toString();
        String dataCurrency = jsonObject.get("currency").toString();
        String dataSupplier = jsonObject.get("supplier").toString();
        String dataCustomer = jsonObject.get("customer")==null?"":jsonObject.get("customer").toString();
        String dataEmployee = jsonObject.get("employee").toString();

        String dataEmails = jsonObject.get("emails").toString();
        String dataAddresses = jsonObject.get("addresses").toString();
        String dataPhoneNumbers = jsonObject.get("phoneNumbers").toString();

        String jsonString = "{\"provider\":\""+ provider +"\",\"updatedAt\":\""+updatedAt +"\",\"itemType\":\""+itemType
                +"\",\"endpointId\":\""+endpointId+"\",\"endpointType\":\""+ endpointType
                +"\",\"excludeFromIndexes\":\""+excludeFromIndexes+"\",\"changeset\":\""+ changeset
                +"\",\"dataId\":\""+dataId+"\",\"dataOrgId\":\""+dataOrgId+"\",\"dataCreatedAt\":\""+dataCreatedAt
                +"\",\"dataCurrency\":\""+dataCurrency+"\",\"dataUpdatedAt\":\""+dataUpdatedAt+"\",\"dataName\":\""+dataName
                +"\",\"dataSupplier\":\""+dataSupplier+"\",\"dataCustomer\":\""+dataCustomer+"\",\"dataEmployee\":\""+dataEmployee
                +"\",\"dataEmails\":"+dataEmails+",\"dataAddresses\":"+dataAddresses+",\"dataPhoneNumbers\":"+dataPhoneNumbers+"}";

        out.output(jsonString);

    }

}
