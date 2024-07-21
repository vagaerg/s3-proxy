/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.aws.proxy.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.server.rest.RequestLoggerController.EventType;
import io.trino.aws.proxy.server.rest.RequestLoggingSession;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.rest.RequestContent;
import io.trino.aws.proxy.spi.rest.RequestHeaders;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import io.trino.aws.proxy.spi.util.ImmutableMultiMap;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.paginators.GetLogEventsIterable;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.aws.proxy.server.rest.RequestLoggerController.EventType.REQUEST_END;
import static io.trino.aws.proxy.server.rest.RequestLoggerController.EventType.REQUEST_START;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoAwsProxyTest
public class TestLogsResource
{
    private static final SigningServiceType FAKE_SERVICE = new SigningServiceType("test");

    private final RequestLoggerController loggerController;
    private final ObjectMapper objectMapper;
    private final CloudWatchLogsClient cloudWatchClient;

    @Inject
    public TestLogsResource(TestingHttpServer httpServer, RequestLoggerController loggerController, TrinoAwsProxyConfig config, @ForTesting Credentials testingCredentials, ObjectMapper objectMapper)
    {
        this.loggerController = requireNonNull(loggerController, "loggerController is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");

        URI uri = UriBuilder.fromUri(httpServer.getBaseUrl()).path(config.getLogsPath()).build();
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(testingCredentials.emulated().accessKey(), testingCredentials.emulated().secretKey());

        cloudWatchClient = CloudWatchLogsClient.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(uri)
                .credentialsProvider(() -> awsBasicCredentials)
                .build();
    }

    @BeforeEach
    public void reset()
    {
        loggerController.clearSavedEntries();
    }

    @Test
    public void testInitialState()
    {
        GetLogEventsRequest request = GetLogEventsRequest.builder().build();
        GetLogEventsResponse response = cloudWatchClient.getLogEvents(request);
        assertThat(response.events()).isEmpty();

        loggerController.clearSavedEntries();

        // log endpoint requests are not included by default - specifically ask for them
        request = GetLogEventsRequest.builder().logStreamName("logs").build();
        response = cloudWatchClient.getLogEvents(request);
        // the request start for this getLogEvents call()
        assertThat(response.events()).hasSize(1);
    }

    @Test
    public void testOrdering()
    {
        final int qty = 500;

        Supplier<GetLogEventsRequest.Builder> builderProc = () -> GetLogEventsRequest.builder().logStreamName(FAKE_SERVICE.serviceName()).limit(qty / 2);

        GetLogEventsResponse response = cloudWatchClient.getLogEvents(builderProc.get().build());
        assertThat(response.events()).isEmpty();

        addFakeRequestLogs(qty);

        GetLogEventsResponse firstResponse = response;
        response = cloudWatchClient.getLogEvents(builderProc.get().build());

        assertThat(response.events()).hasSize(qty / 2);

        Event lastChecked = null;
        Event first = parseEvent(response.events().getFirst());

        for (int i = 1; i < response.events().size(); i += 2) {
            Event previous = parseEvent(response.events().get(i - 1));
            Event check = parseEvent(response.events().get(i));
            assertThat(check).extracting(Event::requestNumber, Event::eventType).containsExactly(previous.requestNumber, REQUEST_START);
            assertThat(previous.eventType).isEqualTo(REQUEST_END);
            assertThat(check.requestNumber).isEqualTo(previous.requestNumber);

            lastChecked = check;
        }
        assertThat(lastChecked).isNotNull();

        String forwardToken = response.nextForwardToken();

        response = cloudWatchClient.getLogEvents(builderProc.get().nextToken(forwardToken).build());
        assertThat(response.events()).hasSize(qty / 2);
        assertThat(parseEvent(response.events().getLast()).requestNumber).isLessThan(lastChecked.requestNumber);

        String backwardToken = firstResponse.nextBackwardToken();

        response = cloudWatchClient.getLogEvents(builderProc.get().nextToken(backwardToken).build());
        assertThat(response.events()).isNotEmpty();
        assertThat(parseEvent(response.events().getFirst())).isEqualTo(first);
        assertThat(response.nextBackwardToken()).isEqualTo(backwardToken);
        assertThat(response.nextForwardToken()).isEqualTo(backwardToken.replace("b/", "f/"));
    }

    @Test
    public void testPaging()
    {
        final int qty = 50;

        addFakeRequestLogs(qty);

        GetLogEventsIterable paginator = cloudWatchClient.getLogEventsPaginator(GetLogEventsRequest.builder().logStreamName(FAKE_SERVICE.serviceName()).startFromHead(true).limit(10).build());
        List<Event> pagedEvents = paginator.stream()
                .flatMap(response -> response.events().stream())
                .map(this::parseEvent)
                .collect(toImmutableList());

        // start/end for each request
        assertThat(pagedEvents).hasSize(qty * 2);

        for (int i = 1; i < pagedEvents.size(); i += 2) {
            Event previous = pagedEvents.get(i - 1);
            Event check = pagedEvents.get(i);
            assertThat(check).extracting(Event::requestNumber, Event::eventType).containsExactly(previous.requestNumber, REQUEST_END);
            assertThat(previous.eventType).isEqualTo(REQUEST_START);
            assertThat(check.requestNumber).isEqualTo(previous.requestNumber);
        }
    }

    private void addFakeRequestLogs(int qty)
    {
        while (qty-- > 0) {
            RequestAuthorization fakeRequestAuthorization = new RequestAuthorization("DUMMY-ACCESS-KEY", "us-east-1", "/hey", ImmutableSet.of(), "dummy", Optional.empty(), Optional.empty());
            Request fakeRequest = new Request(UUID.randomUUID(), fakeRequestAuthorization, Instant.now(), URI.create("http://foo.bar"), RequestHeaders.EMPTY, ImmutableMultiMap.empty(), "GET", RequestContent.EMPTY);
            try (RequestLoggingSession session = loggerController.newRequestSession(fakeRequest, FAKE_SERVICE)) {
                session.logProperty("foo", "bar");
            }
        }
    }

    private record Event(long epoch, long requestNumber, EventType eventType)
    {
        private Event
        {
            requireNonNull(eventType, "eventType is null");
        }
    }

    private Event fromSpec(String spec)
    {
        List<String> parts = Splitter.on('.').splitToList(spec);
        assertThat(parts).hasSize(3);

        long epoch = Long.parseLong(parts.getFirst(), 16);
        long requestNumber = Long.parseLong(parts.get(1), 16);
        EventType eventType = switch (parts.getLast()) {
            case "0" -> REQUEST_START;
            case "1" -> EventType.REQUEST_END;
            default -> Assertions.fail("Unknown event type: " + parts.getLast());
        };

        return new Event(epoch, requestNumber, eventType);
    }

    @SuppressWarnings("unchecked")
    private Event parseEvent(OutputLogEvent event)
    {
        Map<String, String> properties = (Map<String, String>) deserialize(event).get("properties");
        return fromSpec(properties.get("request.eventId"));
    }

    private Map<String, Object> deserialize(OutputLogEvent event)
    {
        try {
            return objectMapper.readValue(event.message(), new TypeReference<>() {});
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
