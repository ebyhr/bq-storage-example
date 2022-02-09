package v1beta1;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageSettings;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;

import java.util.Iterator;

// Set VM options: -Dproject_id={project_id} -Ddataset_id={dataset_id} -Dtable_id={table_id}
// Set environment variable: GOOGLE_APPLICATION_CREDENTIALS={path to credential json}
public class BigQueryV1Beta1
{
    private static final String PROJECT_ID = System.getProperty("project_id");
    private static final String DATASET_ID = System.getProperty("dataset_id");
    private static final String TABLE_ID = System.getProperty("table_id");

    public static void main(String[] args)
            throws Exception
    {
        BigQuery client = BigQueryOptions.newBuilder().build().getService();
        TableInfo tableInfo = client.getTable(DATASET_ID, TABLE_ID);

        BigQueryStorageSettings.Builder clientSettings = BigQueryStorageSettings.newBuilder()
                .setTransportChannelProvider(
                        BigQueryStorageSettings.defaultGrpcTransportProviderBuilder()
                                .setHeaderProvider(FixedHeaderProvider.create("user-agent", "Trino/360"))
                                .build());

        try (BigQueryStorageClient bigQueryStorageClient = BigQueryStorageClient.create(clientSettings.build())) {
            ReadOptions.TableReadOptions.Builder readOptions = ReadOptions.TableReadOptions.newBuilder();

            TableReferenceProto.TableReference tableReference = toTableReference(tableInfo.getTableId());

            Storage.ReadSession readSession = bigQueryStorageClient.createReadSession(
                    Storage.CreateReadSessionRequest.newBuilder()
                            .setParent("projects/" + PROJECT_ID)
                            .setFormat(Storage.DataFormat.AVRO)
                            .setRequestedStreams(3)
                            .setReadOptions(readOptions)
                            .setTableReference(tableReference)
                            // The BALANCED sharding strategy causes the server to
                            // assign roughly the same number of rows to each stream.
                            .setShardingStrategy(Storage.ShardingStrategy.BALANCED)
                            .build());

            for (Storage.Stream stream : readSession.getStreamsList()) {
                Storage.ReadRowsRequest.Builder readRowsRequest = Storage.ReadRowsRequest.newBuilder()
                        .setReadPosition(Storage.StreamPosition.newBuilder()
                                .setStream(Storage.Stream.newBuilder()
                                        .setName(stream.getName())));

                Iterator<Storage.ReadRowsResponse> responses = new ReadRowsHelper(bigQueryStorageClient, readRowsRequest, 3).readRows();
                while (responses.hasNext()) {
                    Storage.ReadRowsResponse response = responses.next();
                    System.out.println(response.getAvroRows().getSerializedBinaryRows());
                }
            }
        }
    }

    private static TableReferenceProto.TableReference toTableReference(TableId tableId)
    {
        return TableReferenceProto.TableReference.newBuilder()
                .setProjectId(tableId.getProject())
                .setDatasetId(tableId.getDataset())
                .setTableId(tableId.getTable())
                .build();
    }
}
