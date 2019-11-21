/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.pwc.dataflow.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;

/**
 * Dataflow template which copies Datastore Entities to a BigQuery table.
 */
public class DatastoreToBigQuery {

    public interface DatastoreToBigQueryOptions
            extends PipelineOptions, DatastoreReadOptions{

        @Description("Pub/sub topic to read data from")
        @Validation.Required
        String getInputTopic();
        void setInputTopic(String value);

        @Description("The BigQuery table spec to write the output to")
        ValueProvider<String> getOutputTableSpec();

        void setOutputTableSpec(ValueProvider<String> value);

        @Validation.Required
        @Description("Temporary directory for BigQuery loading process")
        ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

        void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
    }

    /**
     * Runs a pipeline which reads in Entities from Datastore, passes in the JSON encoded Entities
     * to a Javascript UDF that returns JSON that conforms to the BigQuery TableRow spec and writes
     * the TableRows to BigQuery.
     *
     * @param args arguments to the pipeline
     */
    public static void main(String[] args) {
        DatastoreToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DatastoreToBigQueryOptions.class);

        // Build the table schema for the output table.
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        fields.add(new TableFieldSchema().setName("time").setType("STRING"));
        fields.add(new TableFieldSchema().setName("provider").setType("STRING"));
        fields.add(new TableFieldSchema().setName("org_uid").setType("STRING"));
        fields.add(new TableFieldSchema().setName("item_type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("endpoint_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("endpoint_type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("data_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("data_orgId").setType("STRING"));
        fields.add(new TableFieldSchema().setName("data_amount").setType("STRING"));
        fields.add(new TableFieldSchema().setName("data_currency").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(
                        "ReadFromDatastore",
                        DatastoreIO.v1().read()
                                .withProjectId(options.as(GcpOptions.class).getProject())
                                .withLiteralGqlQuery("select * from NormalizedItem where endpoint_id = '2c92c0f96e446b01016e590101ec1016' and org_uid = 'zuora_anne_001'"))
                .apply("EntityToString", ParDo.of(new EntityToString()))

                //.apply("EntityToJson", ParDo.of(new DatastoreConverters.EntityToJson()))

                .apply("ConvertJsonStringToTableRow",ParDo.of(new JasonStringToTableRow()))
                .apply(
                        "WriteBigQuery",
                        BigQueryIO.writeTableRows()
                                .withSchema(schema)
                                //.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .to(options.getOutputTableSpec()));


        pipeline.run();
    }

    public interface DatastoreReadOptions extends PipelineOptions {
        @Description("GQL Query which specifies what entities to grab")
        ValueProvider<String> getDatastoreReadGqlQuery();
        void setDatastoreReadGqlQuery(ValueProvider<String> datastoreReadGqlQuery);

        @Description("GCP Project Id of where the datastore entities live")
        ValueProvider<String> getDatastoreReadProjectId();
        void setDatastoreReadProjectId(ValueProvider<String> datastoreReadProjectId);

        @Description("Namespace of requested Entties. Set as \"\" for default namespace")
        ValueProvider<String> getDatastoreReadNamespace();
        void setDatastoreReadNamespace(ValueProvider<String> datstoreReadNamespace);
    }

    public interface JavascriptTextTransformerOptions extends PipelineOptions {
        // "Required" annotation is added as a workaround for BEAM-7983.
        @Validation.Required
        @Description("Gcs path to javascript udf source")
        ValueProvider<String> getJavascriptTextTransformGcsPath();

        void setJavascriptTextTransformGcsPath(ValueProvider<String> javascriptTextTransformGcsPath);

        @Validation.Required
        @Description("UDF Javascript Function Name")
        ValueProvider<String> getJavascriptTextTransformFunctionName();

        void setJavascriptTextTransformFunctionName(
                ValueProvider<String> javascriptTextTransformFunctionName);
    }

}

