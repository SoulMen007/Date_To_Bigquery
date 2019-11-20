package com.pwc.dataflow.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.beam.sdk.coders.Coder.Context;


public class JasonStringToTableRow extends DoFn<String, TableRow> {

    @ProcessElement
    public void processElement(@Element String message, OutputReceiver<TableRow> out)
            throws IOException {

        Date nowTime = new Date(System.currentTimeMillis());
        SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd");
        String retStrFormatNowDate = sdFormatter.format(nowTime);
        try {
            JSONObject jsonObject = (JSONObject)(new JSONParser().parse(message));
            System.out.println("==============>: "+jsonObject.toJSONString());
            TableRow row = new TableRow()
                    /*.set("time", retStrFormatNowDate)
                    .set("payment", jsonObject.get("payment"))
                    .set("paymentName", jsonObject.get("paymentName"))
                    .set("amount", jsonObject.get("amount"))
                    .set("effectiveDate", jsonObject.get("effectiveDate"))*/;
            out.output(row);
        } catch (ParseException e) {
            e.printStackTrace();
        }


//        TableRow row = new TableRow()
//                .set("time", retStrFormatNowDate)
//                .set("payment", message);
//        out.output(row);

    }

}