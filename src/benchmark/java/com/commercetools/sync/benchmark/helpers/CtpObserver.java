package com.commercetools.sync.benchmark.helpers;

import io.sphere.sdk.client.SphereRequest;
import io.sphere.sdk.client.metrics.ObservedTotalDuration;
import io.sphere.sdk.commands.CreateCommand;
import io.sphere.sdk.commands.UpdateCommand;
import io.sphere.sdk.http.HttpMethod;

import javax.annotation.Nonnull;
import java.util.Observable;
import java.util.Observer;

/**
 * Observer used by the Simple
 */
public class CtpObserver implements Observer {
    private int totalNumberOfRequests;
    private int totalNumberOfGets;
    private int totalNumberOfPosts;
    private int totalNumberOfUpdates;
    private int totalNumberOfCreates;
    private long averageDuration;
    private long totalDuration;

    public static CtpObserver of() {
        return new CtpObserver();
    }

    @Override
    public void update(@Nonnull final Observable observable, @Nonnull final Object argument) {

        if (argument instanceof ObservedTotalDuration) {

            totalNumberOfRequests++;
            final ObservedTotalDuration observedTotalDuration = (ObservedTotalDuration) argument;

            final SphereRequest<?> request = observedTotalDuration.getRequest();

            if (request instanceof CreateCommand) {
                totalNumberOfCreates++;
            }

            if (request instanceof UpdateCommand) {
                totalNumberOfUpdates++;
            }

            final HttpMethod httpMethod = request.httpRequestIntent().getHttpMethod();

            if (httpMethod == HttpMethod.GET) {
                totalNumberOfGets++;
            } else if (httpMethod == HttpMethod.POST) {
                totalNumberOfPosts++;
            }

            totalDuration += observedTotalDuration.getDurationInMilliseconds();
            averageDuration = totalDuration / totalNumberOfRequests;
        }
    }

    public int getTotalNumberOfRequests() {
        return totalNumberOfRequests;
    }

    public long getAverageDuration() {
        return averageDuration;
    }

    public long getTotalDuration() {
        return totalDuration;
    }

    public int getTotalNumberOfGets() {
        return totalNumberOfGets;
    }

    public int getTotalNumberOfPosts() {
        return totalNumberOfPosts;
    }

    public int getTotalNumberOfUpdates() {
        return totalNumberOfUpdates;
    }

    public int getTotalNumberOfCreates() {
        return totalNumberOfCreates;
    }

    private CtpObserver() {
    }
}
