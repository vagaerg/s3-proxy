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
package io.trino.aws.proxy.spi.rest;

import io.trino.aws.proxy.spi.collections.ImmutableMultiMap;
import io.trino.aws.proxy.spi.collections.MultiMap;
import io.trino.aws.proxy.spi.signing.RequestAuthorization;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public record ParsedS3Request(
        UUID requestId,
        RequestAuthorization requestAuthorization,
        Instant requestDate,
        String bucketName,
        String keyInBucket,
        MultiMap requestHeaders,
        MultiMap queryParameters,
        String httpVerb,
        String rawPath,
        Optional<String> rawQuery,
        RequestContent requestContent)
{
    public ParsedS3Request
    {
        requireNonNull(requestId, "requestId is null");
        requireNonNull(requestAuthorization, "requestAuthorization is null");
        requireNonNull(requestDate, "requestDate is null");
        requireNonNull(bucketName, "bucketName is null");
        requireNonNull(keyInBucket, "keyInBucket is null");
        requestHeaders = ImmutableMultiMap.copyOfCaseInsensitive(requestHeaders);
        queryParameters = ImmutableMultiMap.copyOf(queryParameters);
        requireNonNull(httpVerb, "httpVerb is null");
        requireNonNull(rawPath, "rawPath is null");
        requireNonNull(rawQuery, "rawQuery is null");
        requireNonNull(requestContent, "requestContent is null");
    }
}
