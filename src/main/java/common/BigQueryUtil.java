package common;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Set;

import static com.google.cloud.http.BaseHttpServiceException.UNKNOWN_CODE;
import static com.google.common.base.Throwables.getCausalChain;

public final class BigQueryUtil
{
    private static final Set<String> INTERNAL_ERROR_MESSAGES = ImmutableSet.of(
            "HTTP/2 error code: INTERNAL_ERROR",
            "Connection closed with unknown cause",
            "Received unexpected EOS on DATA frame from server");

    private static final Set<String> INVALID_COLUMN_NAMES = ImmutableSet.of("_partitiondate", "_PARTITIONDATE", "_partitiontime", "_PARTITIONTIME");

    private BigQueryUtil() {}

    public static boolean isRetryable(Throwable cause)
    {
        return getCausalChain(cause).stream().anyMatch(BigQueryUtil::isRetryableInternalError);
    }

    private static boolean isRetryableInternalError(Throwable t)
    {
        if (t instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
            return statusRuntimeException.getStatus().getCode() == Status.Code.INTERNAL &&
                    INTERNAL_ERROR_MESSAGES.stream()
                            .anyMatch(message -> statusRuntimeException.getMessage().contains(message));
        }
        return false;
    }

    public static BigQueryException convertToBigQueryException(BigQueryError error)
    {
        return new BigQueryException(UNKNOWN_CODE, error.getMessage(), error);
    }

    public static boolean validColumnName(String columnName)
    {
        return !INVALID_COLUMN_NAMES.contains(columnName);
    }
}
