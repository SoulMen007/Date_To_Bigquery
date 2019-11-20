package com.pwc.dataflow.example;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityToString extends DoFn<Entity, String> {
   private static final Logger LOG = LoggerFactory.getLogger(EntityToString.class);
    @ProcessElement
    public void processElement(@Element Entity payment, OutputReceiver<String> out)
            throws IOException {

        Map<String, Value> propMap = payment.getPropertiesMap();

        // Grab all relevant fields
        String paymentId = propMap.get("payment_id")==null?"":propMap.get("payment_id").getKeyValue().getPartitionId().getProjectId().toString();
        String paymentName = propMap.get("payment_number")==null?"":propMap.get("payment_number").getStringValue();
        String accountId = propMap.get("account_id")==null?"":propMap.get("account_id").getStringValue();
        String accountNumber = propMap.get("account_number")==null?"":propMap.get("account_number").getStringValue();
        String accountName = propMap.get("account_name")==null?"":propMap.get("account_name").getStringValue();
        Double amount = propMap.get("amount")==null?null:propMap.get("amount").getDoubleValue();
        Long effectiveDate = propMap.get("effective_date")==null?null:propMap.get("effective_date").getTimestampValue().getSeconds();

        String jsonString = "{\"payment\":\""+ paymentId +"\",\"paymentName\":\""+paymentName +"\",\"amount\":\""+amount+"\",\"effectiveDate\":\""+ effectiveDate+"\"}";
        out.output(jsonString);

    }

}
