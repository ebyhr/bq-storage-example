package v1beta1;

import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import common.BigQueryUtil;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

public class ReadRowsHelper
{
    private final BigQueryStorageClient client;
    private final ReadRowsRequest.Builder request;
    private final int maxReadRowsRetries;

    public ReadRowsHelper(BigQueryStorageClient client, ReadRowsRequest.Builder request, int maxReadRowsRetries)
    {
        this.client = requireNonNull(client, "client cannot be null");
        this.request = requireNonNull(request, "request cannot be null");
        this.maxReadRowsRetries = maxReadRowsRetries;
    }

    public Iterator<ReadRowsResponse> readRows()
    {
        Iterator<ReadRowsResponse> serverResponses = fetchResponses(request);
        return new ReadRowsIterator(this, request.getReadPositionBuilder(), serverResponses);
    }

    // In order to enable testing
    protected Iterator<ReadRowsResponse> fetchResponses(ReadRowsRequest.Builder readRowsRequest)
    {
        return client.readRowsCallable()
                .call(readRowsRequest.build())
                .iterator();
    }

    // Ported from https://github.com/GoogleCloudDataproc/spark-bigquery-connector/pull/150
    private static class ReadRowsIterator
            implements Iterator<ReadRowsResponse>
    {
        private final ReadRowsHelper helper;
        private final Storage.StreamPosition.Builder readPosition;
        private Iterator<ReadRowsResponse> serverResponses;
        private long readRowsCount;
        private int retries;

        public ReadRowsIterator(
                ReadRowsHelper helper,
                Storage.StreamPosition.Builder readPosition,
                Iterator<ReadRowsResponse> serverResponses)
        {
            this.helper = helper;
            this.readPosition = readPosition;
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
                    readRowsCount += response.getRowCount();
                    return response;
                }
                catch (Exception e) {
                    // if relevant, retry the read, from the last read position
                    if (BigQueryUtil.isRetryable(e) && retries < helper.maxReadRowsRetries) {
                        serverResponses = helper.fetchResponses(helper.request.setReadPosition(
                                readPosition.setOffset(readRowsCount)));
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