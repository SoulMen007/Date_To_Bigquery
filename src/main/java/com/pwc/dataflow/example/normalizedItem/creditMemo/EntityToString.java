package com.pwc.dataflow.example.normalizedItem.creditMemo;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityToString extends DoFn<Entity, String> {
    private static final Logger LOG = LoggerFactory.getLogger(com.pwc.dataflow.example.EntityToString.class);
    @ProcessElement
    public void processElement(@Element Entity dataStore, OutputReceiver<String> out)
            throws IOException, ParseException {

        Map<String, Value> propMap = dataStore.getPropertiesMap();

        // Grab all relevant fields
        String provider = propMap.get("provider").getStringValue();
        String itemType = propMap.get("item_type").getStringValue();
        String endpointId = propMap.get("endpoint_id").getStringValue();
        String endpointType = propMap.get("endpoint_type").getStringValue();
        String updatedAt = propMap.get("updated_at").getTimestampValue().toString();

        String data = propMap.get("data").getBlobValue().toStringUtf8();
        JSONObject jsonObject = (JSONObject)(new JSONParser().parse(data));
        String dataId = jsonObject.get("id").toString();
        String dataOrgId = jsonObject.get("orgId").toString();
        String dataAmount = jsonObject.get("amount").toString();
        String dataCurrency = jsonObject.get("currency").toString();
        String dataDate = jsonObject.get("date").toString();
        String dataCurrencyRate = jsonObject.get("currencyRate")==null?"":jsonObject.get("currencyRate").toString();
        String dataCreditNoteNumber = jsonObject.get("creditNoteNumber").toString();
        String dataCustomerId = jsonObject.get("customerId").toString();
        String dataJournalId = jsonObject.get("journalId")==null?"":jsonObject.get("journalId").toString();
        String dataLines = jsonObject.get("lines").toString();

        String jsonString = "{\"provider\":\""+ provider +"\",\"updatedAt\":\""+updatedAt +"\",\"itemType\":\""+itemType
                +"\",\"endpointId\":\""+endpointId+"\",\"endpointType\":\""+ endpointType
                +"\",\"dataId\":\""+dataId+"\",\"dataOrgId\":\""+dataOrgId+"\",\"dataAmount\":\""+dataAmount
                +"\",\"dataCurrency\":\""+dataCurrency+"\",\"dataDate\":\""+dataDate+"\",\"dataCurrencyRate\":\""+dataCurrencyRate
                +"\",\"dataCreditNoteNumber\":\""+dataCreditNoteNumber+"\",\"dataCustomerId\":\""+dataCustomerId
                +"\",\"dataJournalId\":\""+dataJournalId+"\",\"dataLines\":"+dataLines+"}";

        out.output(jsonString);

    }

}
