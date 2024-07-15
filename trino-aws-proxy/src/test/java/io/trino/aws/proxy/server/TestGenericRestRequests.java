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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.units.Duration;
import io.trino.aws.proxy.server.rest.TrinoS3ProxyConfig;
import io.trino.aws.proxy.server.testing.TestingCredentialsRolesProvider;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.TestingUtil;
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.server.testing.containers.S3Container.ForS3Container;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTestCommonModules.WithTestingHttpClient;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.aws.proxy.server.testing.TestingUtil.getFileFromStorage;
import static io.trino.aws.proxy.server.testing.TestingUtil.headObjectInStorage;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@TrinoAwsProxyTest(filters = TestGenericRestRequests.Filter.class)
public class TestGenericRestRequests
{
    private final URI baseUri;
    private final TestingCredentialsRolesProvider credentialsRolesProvider;
    private final HttpClient httpClient;
    private final Credentials testingCredentials;
    private final S3Client storageClient;

    private static final String shortLoremIpsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";
    private static final String goodChunkedContent = """
                7b;chunk-signature=20e300fbbad6946a482aaa7de0bdc8f592d4c372306dd746a22d18b7b66b4527\r
                %s\r
                0;chunk-signature=ae4265701a9e0796d671d3339c71db240c0c87b2f6e2f9c6ca7cd781fdcf641a\r
                \r
                """.formatted(shortLoremIpsum);

    // first chunk-signature is bad
    private static final String badChunkedContent1 = """
                7b;chunk-signature=10e300fbbad6946a482aaa7de0bdc8f592d4c372306dd746a22d18b7b66b4527\r
                %s\r
                0;chunk-signature=ae4265701a9e0796d671d3339c71db240c0c87b2f6e2f9c6ca7cd781fdcf641a\r
                \r
                """.formatted(shortLoremIpsum);

    // second chunk-signature is bad
    private static final String badChunkedContent2 = """
                7b;chunk-signature=20e300fbbad6946a482aaa7de0bdc8f592d4c372306dd746a22d18b7b66b4527\r
                %s\r
                0;chunk-signature=9e4265701a9e0796d671d3339c71db240c0c87b2f6e2f9c6ca7cd781fdcf641a\r
                \r
                """.formatted(shortLoremIpsum);

    private static final String goodContent = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Viverra aliquet eget sit amet tellus cras adipiscing. Viverra mauris in aliquam sem fringilla. Facilisis mauris sit amet massa vitae. Mauris vitae ultricies leo integer malesuada. Sed libero enim sed faucibus turpis in eu mi bibendum. Lorem sed risus ultricies tristique nulla aliquet enim. Quis blandit turpis cursus in hac habitasse platea dictumst quisque. Diam maecenas ultricies mi eget mauris pharetra et ultrices neque. Aliquam sem fringilla ut morbi.";

    // first char is different case
    private static final String badContent = "lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Viverra aliquet eget sit amet tellus cras adipiscing. Viverra mauris in aliquam sem fringilla. Facilisis mauris sit amet massa vitae. Mauris vitae ultricies leo integer malesuada. Sed libero enim sed faucibus turpis in eu mi bibendum. Lorem sed risus ultricies tristique nulla aliquet enim. Quis blandit turpis cursus in hac habitasse platea dictumst quisque. Diam maecenas ultricies mi eget mauris pharetra et ultrices neque. Aliquam sem fringilla ut morbi.";

    private static final String goodContentSha256 = "cf091f1003948bfb21f6f166c9ff59a3cc393b106ad744ce9fdae49bdd2d26c9";

    // same as goodContentSha256 with one character replaced
    private static final String goodContentBadSha256 = "bf091f1003948bfb21f6f166c9ff59a3cc393b106ad744ce9fdae49bdd2d26c9";

    // actual SHA256 of badContent
    private static final String badContentSha256 = "29b87372a66b1a091b2b4481ece6fb8564186b82ede3f2e77abaa135330c413f";

    public static class Filter
            extends WithTestingHttpClient
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return super.filter(builder).withProperty("signing-controller.clock.max-drift", new Duration(9999999, TimeUnit.DAYS).toString());
        }
    }

    @Inject
    public TestGenericRestRequests(
            TestingHttpServer httpServer,
            TestingCredentialsRolesProvider credentialsRolesProvider,
            @ForTesting HttpClient httpClient,
            @ForTesting Credentials testingCredentials,
            @ForS3Container S3Client storageClient,
            TrinoS3ProxyConfig trinoS3ProxyConfig)
    {
        baseUri = httpServer.getBaseUrl().resolve(trinoS3ProxyConfig.getS3Path());
        this.credentialsRolesProvider = requireNonNull(credentialsRolesProvider, "credentialsRolesProvider is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.testingCredentials = requireNonNull(testingCredentials, "testingCredentials is null");
        this.storageClient = requireNonNull(storageClient, "storageClient is null");
    }

    @AfterEach
    public void cleanupBuckets()
    {
        TestingUtil.cleanupBuckets(storageClient);
        storageClient.listBuckets().buckets().forEach(bucket -> storageClient.deleteBucket(DeleteBucketRequest.builder().bucket(bucket.name()).build()));
    }

    @Test
    public void testAwsChunkedUpload()
            throws IOException
    {
        Credential credential = new Credential("c160cd8c-8273-4e34-bcf5-3dbddec0c6e0", "464cbc68-2d4f-4e4d-b653-5b1630db9f56");
        Credentials credentials = new Credentials(credential, testingCredentials.remote(), Optional.empty());
        credentialsRolesProvider.addCredentials(credentials);

        storageClient.createBucket(r -> r.bucket("two").build());

        // The chunk signatures are correct but the overall signature in the request is not
        Request goodChunksBadSignature = sampleAwsChunkedUploadRequestBuilder(goodChunkedContent)
                .setHeader("Host", "127.9.9.9:999")
                .build();
        assertThat(httpClient.execute(goodChunksBadSignature, createStatusResponseHandler()).getStatusCode()).isEqualTo(401);

        // Problems with the chunk signature
        assertThat(doAwsChunkedUpload(badChunkedContent1).getStatusCode()).isEqualTo(401);
        assertFileNotInBucket("two", "test");
        assertThat(doAwsChunkedUpload(badChunkedContent2).getStatusCode()).isEqualTo(401);
        assertFileNotInBucket("two", "test");

        // TODO - as per #102, some of the chunks would have been sent to S3 so this assert would fail
        // By this point, no request has succeeded - there should be no content in the bucket

        // Correct chunk and signature
        assertThat(doAwsChunkedUpload(goodChunkedContent).getStatusCode()).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, "two", "test")).isEqualTo(shortLoremIpsum);
    }

    @Test
    public void testAwsChunkedUploadWithMetadata()
            throws IOException
    {
        storageClient.createBucket(r -> r.bucket("two").build());

        testAwsChunkedUploadWithMetadata(ImmutableList.of("gzip,compress,aws-chunked"));
        testAwsChunkedUploadWithMetadata(ImmutableList.of("gzip,compress", "aws-chunked"));
    }

    private void testAwsChunkedUploadWithMetadata(List<String> contentEncodingHeaders)
            throws IOException
    {
        Credential credential = new Credential("c160cd8c-8273-4e34-bcf5-3dbddec0c6e0", "464cbc68-2d4f-4e4d-b653-5b1630db9f56");
        Credentials credentials = new Credentials(credential, testingCredentials.remote(), Optional.empty());
        credentialsRolesProvider.addCredentials(credentials);

        URI uri = UriBuilder.fromUri(baseUri)
                .path("two")
                .path("testWithMetadata")
                .build();

        String content = """
                7b;chunk-signature=d10eae46e386d05132ca7677e4fb0cc2588e02a57818da7938ace696243c10e9\r
                %s\r
                0;chunk-signature=e7a805bc6ed430a7282a002e32ba8a02c4e2c9adfdb43a7a6ded5d5c0bab55cf\r
                \r
                """.formatted(shortLoremIpsum);

        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        Request.Builder requestBuilder = preparePut().setUri(uri)
                .setHeader("Host", "127.0.0.1:52510")
                .setHeader("User-Agent", "aws-sdk-java/2.25.32 Mac_OS_X/14.5 OpenJDK_64-Bit_Server_VM/22.0.1+8 Java/22.0.1 kotlin/2.0.0-release-341 vendor/Eclipse_Adoptium io/sync http/Apache cfg/retry-mode/legacy")
                .setHeader("X-Amz-Date", "20240712T112703Z")
                .setHeader("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .setHeader("x-amz-decoded-content-length", "123")
                .setHeader("Authorization", "AWS4-HMAC-SHA256 Credential=c160cd8c-8273-4e34-bcf5-3dbddec0c6e0/20240712/us-east-1/s3/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-encoding;content-length;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-meta-metadata-key, Signature=2a6854bd147d1fcbeddc02c04a6e241eacf05f42f79a4819e18b4d118d6c54cd")
                .setHeader("amz-sdk-invocation-id", "c7135c74-ad43-4899-1409-331874a6bd78")
                .setHeader("amz-sdk-request", "attempt=1; max=4")
                .setHeader("Content-Length", "296")
                .setHeader("Content-Type", "text/plain;charset=UTF-8")
                .setHeader("x-amz-meta-metadata-key", "metadata-value")
                .setBodyGenerator(createStaticBodyGenerator(content, StandardCharsets.UTF_8));
        contentEncodingHeaders.forEach(contentEncodingHeader -> requestBuilder.addHeader("Content-Encoding", contentEncodingHeader));
        Request request = requestBuilder.build();
        assertThat(httpClient.execute(request, createStatusResponseHandler()).getStatusCode()).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, "two", "testWithMetadata")).isEqualTo(shortLoremIpsum);
        HeadObjectResponse headObjectResponse = headObjectInStorage(storageClient, "two", "testWithMetadata");
        assertThat(headObjectResponse.sdkHttpResponse().statusCode()).isEqualTo(200);
        assertThat(headObjectResponse.contentType()).isEqualTo("text/plain;charset=UTF-8");
        assertThat(headObjectResponse.contentEncoding()).isEqualTo("gzip,compress");
        assertThat(headObjectResponse.metadata()).containsEntry("metadata-key", "metadata-value");
    }

    @Test
    public void testPutObject()
            throws IOException
    {
        storageClient.createBucket(r -> r.bucket("foo").build());

        Credential credential = new Credential("df4899b1-9026-4c51-a3f3-38fffa236748", "2a142f69-d384-4739-8733-2977f73e2d2c");
        Credentials credentials = new Credentials(credential, testingCredentials.remote(), Optional.empty());
        credentialsRolesProvider.addCredentials(credentials);

        // Content does not match its hash
        assertThat(doPutObject(badContent, goodContentSha256).getStatusCode()).isEqualTo(401);
        assertFileNotInBucket("foo", "bar");

        assertThat(doPutObject(goodContent, goodContentBadSha256).getStatusCode()).isEqualTo(401);
        assertFileNotInBucket("foo", "bar");

        assertThat(doPutObject(badContent, goodContentBadSha256).getStatusCode()).isEqualTo(401);
        assertFileNotInBucket("foo", "bar");

        // Content matches its hash, but is different from the hash that was used in the signature
        assertThat(doPutObject(badContent, badContentSha256).getStatusCode()).isEqualTo(401);
        assertFileNotInBucket("foo", "bar");

        // Correct content and hash
        assertThat(doPutObject(goodContent, goodContentSha256).getStatusCode()).isEqualTo(200);
        assertThat(getFileFromStorage(storageClient, "foo", "bar")).isEqualTo(goodContent);
    }

    private StatusResponse doPutObject(String content, String sha256)
    {
        URI uri = UriBuilder.fromUri(baseUri)
                .path("foo")
                .path("bar")
                .build();

        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        Request request = preparePut().setUri(uri)
                .setHeader("Host", "127.0.0.1:10064")
                .setHeader("Accept-Encoding", "identity")
                .setHeader("Content-Type", "text/plain")
                .setHeader("User-Agent", "aws-cli/2.15.53 md/awscrt#0.19.19 ua/2.0 os/macos#22.6.0 md/arch#x86_64 lang/python#3.11.9 md/pyimpl#CPython cfg/retry-mode#standard md/installer#source md/prompt#off md/command#s3.cp")
                .setHeader("Content-MD5", "cAm9NzhZWdMdTMJEogaTZQ==")
                .setHeader("Expect", "100-continue")
                .setHeader("X-Amz-Date", "20240617T114456Z")
                .setHeader("X-Amz-Content-SHA256", sha256)
                .setHeader("Authorization", "AWS4-HMAC-SHA256 Credential=df4899b1-9026-4c51-a3f3-38fffa236748/20240617/us-east-1/s3/aws4_request, SignedHeaders=content-md5;content-type;host;x-amz-content-sha256;x-amz-date, Signature=89fffb33b584a661ec05906a3da4975903e13c46e030b4231c53711c36a9f78e")
                .setHeader("Content-Length", "582")
                .setBodyGenerator(createStaticBodyGenerator(content, StandardCharsets.UTF_8))
                .build();

        return httpClient.execute(request, createStatusResponseHandler());
    }

    private Request.Builder sampleAwsChunkedUploadRequestBuilder(String content)
    {
        // values discovered from an AWS CLI request sent to a dummy local HTTP server
        return preparePut().setUri(UriBuilder.fromUri(baseUri).path("two").path("test").build())
                .setHeader("Host", "127.0.0.1:62820")
                .setHeader("User-Agent", "aws-sdk-java/2.25.32 Mac_OS_X/13.6.7 OpenJDK_64-Bit_Server_VM/22.0.1+8-16 Java/22.0.1 kotlin/1.9.23-release-779 vendor/Oracle_Corporation io/sync http/Apache cfg/retry-mode/legacy")
                .setHeader("X-Amz-Date", "20240618T080640Z")
                .setHeader("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                .setHeader("x-amz-decoded-content-length", "123")
                .setHeader("Authorization", "AWS4-HMAC-SHA256 Credential=c160cd8c-8273-4e34-bcf5-3dbddec0c6e0/20240618/us-east-1/s3/aws4_request, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;content-encoding;content-length;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length, Signature=3bdce17ef4446ba2900c8f90b2e8ee812ccfa4625abb67030fae01dd1a9d347b")
                .setHeader("Content-Encoding", "aws-chunked")
                .setHeader("amz-sdk-invocation-id", "0c59609c-1c7b-e503-0583-b0271b5e8b21")
                .setHeader("amz-sdk-request", "attempt=1; max=4")
                .setHeader("Content-Length", "296")
                .setHeader("Content-Type", "text/plain")
                .setBodyGenerator(createStaticBodyGenerator(content, StandardCharsets.UTF_8));
    }
    private StatusResponse doAwsChunkedUpload(String content)
    {
        return httpClient.execute(sampleAwsChunkedUploadRequestBuilder(content).build(), createStatusResponseHandler());
    }

    private void assertFileNotInBucket(String bucket, String key)
    {
        assertThatExceptionOfType(S3Exception.class)
                .isThrownBy(() -> getFileFromStorage(storageClient, bucket, key))
                .extracting(S3Exception::statusCode)
                .isEqualTo(404);
    }
}
