package com.pwc.dataflow.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


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
                    .set("time", retStrFormatNowDate)
                    .set("provider", jsonObject.get("provider"))
                    .set("org_uid", jsonObject.get("orgUid"))
                    .set("item_type", jsonObject.get("itemType"))
                    .set("endpoint_id", jsonObject.get("endpointId"))
                    .set("endpoint_type", jsonObject.get("endpointType"))
                    .set("data_id",jsonObject.get("dataId"))
                    .set("data_orgId", jsonObject.get("dataOrgId"))
                    .set("data_amount", jsonObject.get("dataAmount"))
                    .set("data_currency", jsonObject.get("dataCurrency"));
            out.output(row);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}