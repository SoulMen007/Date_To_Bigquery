package com.pwc.dataflow.example.normalizedItem.account;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;


public class JasonStringToTableRow extends DoFn<String, TableRow> {

    @ProcessElement
    public void processElement(@Element String message, OutputReceiver<TableRow> out)
            throws IOException {

        Date nowTime = new Date(System.currentTimeMillis());
        SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd");
        String retStrFormatNowDate = sdFormatter.format(nowTime);
        try {
            JSONObject jsonObject = (JSONObject)(new JSONParser().parse(message));
            TableRow row = new TableRow();
            row.set("create_time", retStrFormatNowDate);
            Iterator iter = jsonObject.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                row.set(entry.getKey().toString(), entry.getValue().toString());

            }
            out.output(row);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}