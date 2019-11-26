package com.pwc.dataflow.example.normalizedItem.invoice;

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
                    .set("provider", jsonObject.get("provider").toString())
                    .set("updated_at", jsonObject.get("updatedAt").toString())
                    .set("item_type", jsonObject.get("itemType").toString())
                    .set("endpoint_id", jsonObject.get("endpointId").toString())
                    .set("endpoint_type", jsonObject.get("endpointType").toString())
                    .set("exclude_from_indexes", jsonObject.get("excludeFromIndexes").toString())
                    .set("changeset", jsonObject.get("changeset").toString())
                    .set("data_id",jsonObject.get("dataId").toString())
                    .set("data_orgId", jsonObject.get("dataOrgId").toString())
                    .set("data_createdAt", jsonObject.get("dataCreatedAt").toString())
                    .set("data_updatedAt", jsonObject.get("dataUpdatedAt").toString())
                    .set("data_invoiceNumber", jsonObject.get("dataInvoiceNumber").toString())
                    .set("data_currency", jsonObject.get("dataCurrency").toString())
                    .set("data_date", jsonObject.get("dataDate").toString())
                    .set("data_dueDate", jsonObject.get("dataDueDate").toString())
                    .set("data_billingAddress", jsonObject.get("dataBillingAddress").toString())
                    .set("data_shipFromAddress", jsonObject.get("dataShipFromAddress").toString())
                    .set("data_shippingAddress", jsonObject.get("dataShippingAddress").toString())
                    .set("data_currencyRate", jsonObject.get("dataCurrencyRate").toString())
                    .set("data_netTotal", jsonObject.get("dataNetTotal").toString())
                    .set("data_taxTotal", jsonObject.get("dataTaxTotal").toString())
                    .set("data_dueTotal", jsonObject.get("dataDueTotal").toString())
                    .set("data_applyTaxAfterDiscount", jsonObject.get("dataApplyTaxAfterDiscount").toString())
                    .set("data_discountTotal", jsonObject.get("dataDiscountTotal"))
                    .set("data_outstandingTotal", jsonObject.get("dataOutstandingTotal").toString())
                    .set("data_taxExemptTotal", jsonObject.get("dataTaxExemptTotal").toString())
                    .set("data_contactId", jsonObject.get("dataContactId").toString())
                    .set("data_journalId", jsonObject.get("dataJournalId").toString())
                    .set("data_lines", jsonObject.get("dataLines").toString());
            out.output(row);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}