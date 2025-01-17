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
package io.trino.s3.proxy.server.rest;

import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.InputStream;

import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static java.util.Objects.requireNonNull;

class StreamingResponseHandler
        implements ResponseHandler<Void, RuntimeException>
{
    private final AsyncResponse asyncResponse;

    StreamingResponseHandler(AsyncResponse asyncResponse)
    {
        this.asyncResponse = requireNonNull(asyncResponse, "asyncResponse is null");
    }

    @Override
    public Void handleException(Request request, Exception exception)
            throws RuntimeException
    {
        throw propagate(request, exception);
    }

    @Override
    public Void handle(Request request, Response response)
            throws RuntimeException
    {
        StreamingOutput streamingOutput = output -> {
            InputStream inputStream = response.getInputStream();

            // HttpClient/Jersey timeouts control behavior. The configured HttpClient idle timeout
            // controls whether the InputStream will time out. Jersey configuration controls
            // OutputStream and general request timeouts.
            inputStream.transferTo(output);
            output.flush();
        };

        jakarta.ws.rs.core.Response.ResponseBuilder responseBuilder = jakarta.ws.rs.core.Response.status(response.getStatusCode()).entity(streamingOutput);
        response.getHeaders()
                .keySet()
                .stream()
                .map(HeaderName::toString)
                .forEach(name -> response.getHeaders(name).forEach(value -> responseBuilder.header(name, value)));

        // this will block until StreamingOutput completes
        asyncResponse.resume(responseBuilder.build());

        return null;
    }
}
