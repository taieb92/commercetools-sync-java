package com.commercetools.sync.benchmark.helpers;

import io.sphere.sdk.client.SphereRequest;
import io.sphere.sdk.client.metrics.ObservedTotalDuration;
import io.sphere.sdk.commands.CreateCommand;
import io.sphere.sdk.commands.UpdateCommand;
import io.sphere.sdk.http.HttpMethod;
import io.sphere.sdk.queries.Query;

import javax.annotation.Nonnull;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Observer used by the Simple
 */
public class CtpObserver implements Observer {
    private AtomicInteger totalNumberOfRequests = new AtomicInteger();
    private AtomicInteger totalNumberOfUpdatesRequests = new AtomicInteger();
    private AtomicInteger totalNumberOfCreatesRequests = new AtomicInteger();
    private AtomicInteger totalNumberOfQueryRequests = new AtomicInteger();
    private AtomicInteger totalNumberOfGets = new AtomicInteger();
    private AtomicInteger totalNumberOfPosts = new AtomicInteger();
    private AtomicLong averageDuration = new AtomicLong();
    private AtomicLong totalDuration = new AtomicLong();

    public static CtpObserver of() {
        return new CtpObserver();
    }

    @Override
    public void update(@Nonnull final Observable observable, @Nonnull final Object argument) {

        if (argument instanceof ObservedTotalDuration) {

            totalNumberOfRequests.incrementAndGet();
            final ObservedTotalDuration observedTotalDuration = (ObservedTotalDuration) argument;

            final SphereRequest<?> request = observedTotalDuration.getRequest();

            if (request instanceof CreateCommand) {
                totalNumberOfCreatesRequests.incrementAndGet();
            }

            if (request instanceof UpdateCommand) {
                totalNumberOfUpdatesRequests.incrementAndGet();
            }

            if (request instanceof Query) {
                totalNumberOfQueryRequests.incrementAndGet();
            }

            final HttpMethod httpMethod = request.httpRequestIntent().getHttpMethod();

            if (httpMethod == HttpMethod.GET) {
                totalNumberOfGets.incrementAndGet();
            } else if (httpMethod == HttpMethod.POST) {
                totalNumberOfPosts.incrementAndGet();
            }

            totalDuration.addAndGet(observedTotalDuration.getDurationInMilliseconds());
            averageDuration =  new AtomicLong(totalDuration.get() / totalNumberOfRequests.get());
        }
    }

    public AtomicInteger getTotalNumberOfRequests() {
        return totalNumberOfRequests;
    }

    public AtomicLong getAverageDuration() {
        return averageDuration;
    }

    public AtomicLong getTotalDuration() {
        return totalDuration;
    }

    public AtomicInteger getTotalNumberOfGets() {
        return totalNumberOfGets;
    }

    public AtomicInteger getTotalNumberOfPosts() {
        return totalNumberOfPosts;
    }

    public AtomicInteger getTotalNumberOfUpdateRequests() {
        return totalNumberOfUpdatesRequests;
    }

    public AtomicInteger getTotalNumberOfCreateRequests() {
        return totalNumberOfCreatesRequests;
    }

    public AtomicInteger getTotalNumberOfQueryRequests() {
        return totalNumberOfQueryRequests;
    }

    private CtpObserver() {
    }

}
