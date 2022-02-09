package v1;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import common.BigQueryUtil;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

public class ReadRowsHelper
{
    private final BigQueryReadClient client;
    private final String streamName;
    private final int maxReadRowsRetries;

    public ReadRowsHelper(BigQueryReadClient client, String streamName, int maxReadRowsRetries)
    {
        this.client = requireNonNull(client, "client cannot be null");
        this.streamName = requireNonNull(streamName, "streamName cannot be null");
        this.maxReadRowsRetries = maxReadRowsRetries;
    }

    public Iterator<ReadRowsResponse> readRows()
    {
        Iterator<ReadRowsResponse> serverResponses = fetchResponses(0);
        return new ReadRowsIterator(this, serverResponses);
    }

    // In order to enable testing
    protected Iterator<ReadRowsResponse> fetchResponses(long offset)
    {
        return client.readRowsCallable()
                .call(ReadRowsRequest.newBuilder()
                        .setReadStream(streamName)
                        .setOffset(offset)
                        .build())
                .iterator();
    }

    // Ported from https://github.com/GoogleCloudDataproc/spark-bigquery-connector/pull/150
    private static class ReadRowsIterator
            implements Iterator<ReadRowsResponse>
    {
        private final ReadRowsHelper helper;
        private long nextOffset;
        private Iterator<ReadRowsResponse> serverResponses;
        private int retries;

        public ReadRowsIterator(
                ReadRowsHelper helper,
                Iterator<ReadRowsResponse> serverResponses)
        {
            this.helper = helper;
            this.serverResponses = serverResponses;
        }

        @Override
        public boolean hasNext()
        {
            return serverResponses.hasNext();
        }

        @Override
        public ReadRowsResponse next()
        {
            do {
                try {
                    ReadRowsResponse response = serverResponses.next();
                    nextOffset += response.getRowCount();
                    return response;
                }
                catch (Exception e) {
                    // if relevant, retry the read, from the last read position
                    if (BigQueryUtil.isRetryable(e) && retries < helper.maxReadRowsRetries) {
                        serverResponses = helper.fetchResponses(nextOffset);
                        retries++;
                    }
                    else {
                        helper.client.close();
                        throw e;
                    }
                }
            }
            while (serverResponses.hasNext());

            throw new NoSuchElementException("No more server responses");
        }
    }
}
