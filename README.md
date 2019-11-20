# gcp_data_to_bigquery
Raw data and Normalised data to Bigquery pipeline-part 1.

## Environment Setup

You need maven and Java SDK 8 or latter installed. 

### Environment Variables:
    export GOOGLE_APPLICATION_CREDENTIALS="/Users/ssun206/Downloads/acuit-zoran-wins-sandbox-99fcc0065b05.json"

### Running The Sync Pipeline on localhost:

Set gcloud project 

    gcloud config set project acuit-zoran-wins-sandbox

Set the gcloud default credentials via:

    gcloud auth application-default login
    
Start the gcp_data_to_bigquery app:
   
    mvn -Pdataflow-runner compile exec:java \
 -Dexec.mainClass=com.pwc.dataflow.example.DatastoreToBigQuery  \
   -Dexec.args="--project=acuit-zoran-wins-sandbox \
     --inputTopic=projects/acuit-zoran-wins-sandbox/topics/test \
     --outputTableSpec=acuit-zoran-wins-sandbox:sam.data_to_bigquery \
     --runner=DataflowRunner"
    



