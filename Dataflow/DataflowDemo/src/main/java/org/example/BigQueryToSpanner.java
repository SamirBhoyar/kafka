package org.example;


import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import static org.apache.beam.sdk.options.PipelineOptionsFactory.as;

public class BigQueryToSpanner {

//    public interface CustomPipelineOptions extends PipelineOptions {
//        @Description("Instance ID for Spanner")
//        ValueProvider<String> getInstanceId();
//        void setInstanceId(ValueProvider<String> instanceId);
//
//        @Description("Database ID for Spanner")
//        ValueProvider<String> getDatabaseId();
//        void setDatabaseId(ValueProvider<String> databaseId);
//    }
    public static void main(String[] args) {
        // Create the Beam pipeline
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().create();
//                .as(CustomPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        // BigQuery query or table specification
        String bigQueryTable = "bigquery-public-data.google_trends.top_terms"; //"your-project-id:your-dataset-id.your-table-id"

        // Read data from BigQuery
        PCollection<TableRow> rows= pipeline.apply("ReadFromBigQuery", BigQueryIO.readTableRows()
                        .from(bigQueryTable)
                        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ));

        //Process the data(optional)
        rows.apply("processElement", ParDo.of(new DoFn<TableRow, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        String[] row= c.element().split();
                        String name =(String) row[0];
//                        Long age = (Long) row[1];
                        System.out.println("name" + name );
                    }
                }));

//                .apply("TransformToMutations", MapElements.into(TypeDescriptor.of(Mutation.class))
//                        .via(new SimpleFunction<TableRow, Mutation>() {
//                            @Override
//                            public Mutation apply(TableRow row) {
//                                // Transform TableRow from BigQuery into Mutation for Spanner
//                                return Mutation.newInsertOrUpdateBuilder("your-spanner-table")
//                                        .set("column1").to(Value.string((String) row.get("column1")))
//                                        .set("column2").to(Value.int64((Long) row.get("column2")))
//                                        .build();
//                            }
//                        }))
                // Write the transformed data into Spanner
//                .apply("WriteToSpanner", SpannerIO.write()
//                        .withInstanceId("your-spanner-instance")
//                        .withDatabaseId("your-spanner-database"));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }
}

//===========
//Run the pipeline on Dataflow with the necessary options (e.g., project, region):
//
//java -jar target/your-pipeline-jar.jar \
//        --runner=DataflowRunner \
//        --project=My First Project \
//        --region=europe-west1 \
//        --instanceId=dataflowdemo \
//        --databaseId=demodf \
//        --gcpTempLocation=gs://your-bucket/temp
