package v1;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;

import java.util.Iterator;

import static java.lang.String.format;

// Set VM options: -Dproject_id={project_id} -Ddataset_id={dataset_id} -Dtable_id={table_id}
// Set environment variable: GOOGLE_APPLICATION_CREDENTIALS={path to credential json}
public class BigQueryV1
{
    private static final String PROJECT_ID = System.getProperty("project_id");
    private static final String DATASET_ID = System.getProperty("dataset_id");
    private static final String TABLE_ID = System.getProperty("table_id");

    public static void main(String[] args)
            throws Exception
    {
        BigQuery client = BigQueryOptions.newBuilder().build().getService();
        TableInfo tableDetails = client.getTable(DATASET_ID, TABLE_ID);

        BigQueryReadSettings.Builder clientSettings = BigQueryReadSettings.newBuilder()
                .setTransportChannelProvider(
                        BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
                                .setHeaderProvider(FixedHeaderProvider.create("user-agent", "Trino/368"))
                                .build());

        try (BigQueryReadClient bigQueryReadClient = BigQueryReadClient.create(clientSettings.build())) {
            ReadSession.TableReadOptions.Builder readOptions = ReadSession.TableReadOptions.newBuilder();

            TableId tableId = tableDetails.getTableId();
            ReadSession readSession = bigQueryReadClient.createReadSession(
                    CreateReadSessionRequest.newBuilder()
                            .setParent("projects/" + PROJECT_ID)
                            .setReadSession(ReadSession.newBuilder()
                                    .setDataFormat(DataFormat.AVRO)
                                    .setTable(format("projects/%s/datasets/%s/tables/%s", tableId.getProject(), tableId.getDataset(), tableId.getTable()))
                                    .setReadOptions(readOptions))
                            .build());

            for (ReadStream stream : readSession.getStreamsList()) {
                Iterator<ReadRowsResponse> responses = new ReadRowsHelper(bigQueryReadClient, stream.getName(), 3).readRows();
                while (responses.hasNext()) {
                    ReadRowsResponse response = responses.next();
                    System.out.println(response.getAvroRows().getSerializedBinaryRows());
                }
            }
        }
    }
}
