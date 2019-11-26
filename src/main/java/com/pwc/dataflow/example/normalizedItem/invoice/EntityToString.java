package com.pwc.dataflow.example.normalizedItem.invoice;

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
        String dataInvoiceNumber = jsonObject.get("invoiceNumber").toString();
        String dataDate = jsonObject.get("date").toString();
        String dataCurrency = jsonObject.get("currency").toString();
        String dataDueDate = jsonObject.get("dueDate")==null?"":jsonObject.get("dueDate").toString();
        String dataBillingAddress = jsonObject.get("billingAddress")==null?"":jsonObject.get("billingAddress").toString();
        String dataShipFromAddress = jsonObject.get("shipFromAddress")==null?"":jsonObject.get("shipFromAddress").toString();
        String dataShippingAddress = jsonObject.get("shippingAddress")==null?"":jsonObject.get("shippingAddress").toString();
        String dataCurrencyRate = jsonObject.get("currencyRate")==null?"":jsonObject.get("currencyRate").toString();
        String dataNetTotal = jsonObject.get("netTotal").toString();
        String dataTaxTotal = jsonObject.get("taxTotal").toString();
        String dataDueTotal = jsonObject.get("dueTotal").toString();
        String dataApplyTaxAfterDiscount = jsonObject.get("applyTaxAfterDiscount").toString();
        String dataDiscountTotal = jsonObject.get("discountTotal").toString();
        String dataOutstandingTotal = jsonObject.get("outstandingTotal").toString();
        String dataTaxExemptTotal = jsonObject.get("taxExemptTotal").toString();
        String dataContactId = jsonObject.get("contactId")==null?"":jsonObject.get("contactId").toString();
        String dataJournalId = jsonObject.get("journalId")==null?"":jsonObject.get("journalId").toString();
        String dataLines = jsonObject.get("lines").toString();

        String jsonString = "{\"provider\":\""+ provider +"\",\"updatedAt\":\""+updatedAt +"\",\"itemType\":\""+itemType
                +"\",\"endpointId\":\""+endpointId+"\",\"endpointType\":\""+ endpointType
                +"\",\"excludeFromIndexes\":\""+excludeFromIndexes+"\",\"changeset\":\""+ changeset
                +"\",\"dataId\":\""+dataId+"\",\"dataOrgId\":\""+dataOrgId+"\",\"dataCreatedAt\":\""+dataCreatedAt
                +"\",\"dataCurrency\":\""+dataCurrency+"\",\"dataUpdatedAt\":\""+dataUpdatedAt+"\",\"dataInvoiceNumber\":\""+dataInvoiceNumber
                +"\",\"dataDate\":\""+dataDate+"\",\"dataDueDate\":\""+dataDueDate+"\",\"dataBillingAddress\":\""+dataBillingAddress
                +"\",\"dataShipFromAddress\":\""+dataShipFromAddress+"\",\"dataShippingAddress\":\""+dataShippingAddress+"\",\"dataCurrencyRate\":\""+dataCurrencyRate
                +"\",\"dataNetTotal\":\""+dataNetTotal+"\",\"dataTaxTotal\":\""+dataTaxTotal+"\",\"dataDueTotal\":\""+dataDueTotal
                +"\",\"dataApplyTaxAfterDiscount\":\""+dataApplyTaxAfterDiscount+"\",\"dataDiscountTotal\":\""+dataDiscountTotal+"\",\"dataOutstandingTotal\":\""+dataOutstandingTotal
                +"\",\"dataTaxExemptTotal\":\""+dataTaxExemptTotal+"\",\"dataContactId\":\""+dataContactId+"\",\"dataJournalId\":\""+dataJournalId
                +"\",\"dataLines\":"+dataLines+"}";

        out.output(jsonString);

    }

}
