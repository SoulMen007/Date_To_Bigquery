package com.pwc.dataflow.example.normalizedItem.account;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class JasonStringToTableRow extends DoFn<String, TableRow> {

    @ProcessElement
    public void processElement(@Element String message, OutputReceiver<TableRow> out)
            throws IOException {

        Date nowTime = new Date(System.currentTimeMillis());
        SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd");
        String retStrFormatNowDate = sdFormatter.format(nowTime);
        try {
            JSONObject jsonObject = (JSONObject)(new JSONParser().parse(message));
            TableRow row = new TableRow()
                    .set("create_time", retStrFormatNowDate)
                    .set("provider", jsonObject.get("provider"))
                    .set("updated_at", jsonObject.get("updatedAt"))
                    .set("item_type", jsonObject.get("itemType"))
                    .set("endpoint_id", jsonObject.get("endpointId"))
                    .set("endpoint_type", jsonObject.get("endpointType"))
                    .set("exclude_from_indexes", jsonObject.get("excludeFromIndexes"))
                    .set("changeset", jsonObject.get("changeset"))
                    .set("data_id",jsonObject.get("dataId"))
                    .set("data_orgId", jsonObject.get("dataOrgId"))
                    .set("data_createdAt", jsonObject.get("dataCreatedAt"))
                    .set("data_updatedAt", jsonObject.get("dataUpdatedAt"))
                    .set("data_name", jsonObject.get("dataName"))
                    .set("data_currency", jsonObject.get("dataCurrency"))
                    .set("data_supplier", jsonObject.get("dataSupplier"))
                    .set("data_customer", jsonObject.get("dataCustomer"))
                    .set("data_employee", jsonObject.get("dataEmployee"))
                    .set("data_emails", jsonObject.get("dataEmails").toString())
                    .set("data_addresses", jsonObject.get("dataAddresses").toString())
                    .set("data_phoneNumbers", jsonObject.get("dataPhoneNumbers").toString());
            out.output(row);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}