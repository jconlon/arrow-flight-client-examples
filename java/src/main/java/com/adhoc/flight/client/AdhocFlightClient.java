/*
 * Copyright (C) 2017-2021 Dremio Corporation
 *
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

package com.adhoc.flight.client;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.adhoc.flight.utils.QueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.adhoc.flight.client.EncryptedConnectionUtils.getCertificateStream;

/**
 * Adhoc Flight Client encapsulating an active FlightClient and a corresponding
 * CredentialCallOption with a bearer token for subsequent FlightRPC requests.
 */
public class AdhocFlightClient implements AutoCloseable {
  private final FlightClient client;
  private final CredentialCallOption bearerToken;
  private final static Logger LOGGER = LoggerFactory.getLogger(AdhocFlightClient.class);

  public AdhocFlightClient(FlightClient client, CredentialCallOption bearerToken) {
    this.client = client;
    this.bearerToken = bearerToken;
  }

  /**
   * Creates a FlightClient connected to the Dremio server with encrypted TLS
   * connection.
   *
   * @param allocator        the BufferAllocator.
   * @param host             the Dremio host.
   * @param port             the Dremio port where Flight Server Endpoint is
   *                         running on.
   * @param user             the Dremio username.
   * @param pass             the corresponding password.
   * @param keyStorePath     path to the JKS.
   * @param keyStorePass     the password to the JKS.
   * @param clientProperties the client properties to set during authentication.
   * @return an AdhocFlightClient encapsulating the client instance and
   *         CallCredentialOption with bearer token for subsequent FlightRPC
   *         requests.
   * @throws Exception RuntimeException if unable to access JKS with provided
   *                   information.
   */
  public static AdhocFlightClient getEncryptedClient(BufferAllocator allocator, String host, int port, String user,
      String pass, String keyStorePath, String keyStorePass, boolean verifyServer, HeaderCallOption clientProperties)
      throws Exception {
    // Create a new instance of ClientIncomingAuthHeaderMiddleware.Factory. This
    // factory creates
    // new instances of ClientIncomingAuthHeaderMiddleware. The middleware processes
    // username/password and bearer token authorization header authentication for
    // this Flight Client.
    final ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(
        new ClientBearerHeaderHandler());

    // Adds ClientIncomingAuthHeaderMiddleware.Factory instance to the FlightClient
    // builder.
    final FlightClient.Builder clientBuilder = FlightClient.builder();
    InputStream stream = getCertificateStream(keyStorePath, keyStorePass);
    if (verifyServer) {
      clientBuilder.allocator(allocator).location(Location.forGrpcTls(host, port)).intercept(factory).useTls()
          .verifyServer(false);
    } else {
      clientBuilder.allocator(allocator).location(Location.forGrpcTls(host, port)).intercept(factory).useTls()
          .trustedCertificates(stream);
    }

    final FlightClient client = clientBuilder.build();
    return new AdhocFlightClient(client, authenticate(client, user, pass, factory, clientProperties));
  }

  /**
   * Creates a FlightClient connected to the Dremio server with an unencrypted
   * connection.
   *
   * @param allocator        the BufferAllocator.
   * @param host             the Dremio host.
   * @param port             the Dremio port where Flight Server Endpoint is
   *                         running on.
   * @param user             the Dremio username.
   * @param pass             the corresponding password.
   * @param clientProperties the client properties to set during authentication.
   * @return an AdhocFlightClient encapsulating the client instance and
   *         CallCredentialOption with bearer token for subsequent FlightRPC
   *         requests.
   */
  public static AdhocFlightClient getBasicClient(BufferAllocator allocator, String host, int port, String user,
      String pass, HeaderCallOption clientProperties) {
    // Create a new instance of ClientIncomingAuthHeaderMiddleware.Factory. This
    // factory creates
    // new instances of ClientIncomingAuthHeaderMiddleware. The middleware processes
    // username/password and bearer token authorization header authentication for
    // this Flight Client.
    final ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(
        new ClientBearerHeaderHandler());

    // Adds ClientIncomingAuthHeaderMiddleware.Factory instance to the FlightClient
    // builder.
    final FlightClient client = FlightClient.builder().allocator(allocator)
        .location(Location.forGrpcInsecure(host, port)).intercept(factory).build();
    return new AdhocFlightClient(client, authenticate(client, user, pass, factory, clientProperties));
  }

  /**
   * Helper method to authenticate provided FlightClient instance against a Dremio
   * Flight Server Endpoint.
   *
   * @param client           the FlightClient instance to connect to Dremio.
   * @param user             the Dremio username.
   * @param pass             the corresponding Dremio password
   * @param factory          the factory to create
   *                         ClientIncomingAuthHeaderMiddleware.
   * @param clientProperties client properties to set during authentication.
   * @return CredentialCallOption encapsulating the bearer token to use in
   *         subsequent requests.
   */
  public static CredentialCallOption authenticate(FlightClient client, String user, String pass,
      ClientIncomingAuthHeaderMiddleware.Factory factory, HeaderCallOption clientProperties) {
    final List<CallOption> callOptions = new ArrayList<>();

    // Add CredentialCallOption for authentication.
    // A CredentialCallOption is instantiated with an instance of
    // BasicAuthCredentialWriter.
    // The BasicAuthCredentialWriter takes in username and password pair, encodes
    // the pair and
    // insert the credentials into the Authorization header to authenticate with the
    // server.
    callOptions.add(new CredentialCallOption(new BasicAuthCredentialWriter(user, pass)));

    // Note: Dremio client properties "routing-tag" and "routing-queue" can only be
    // set during
    // initial authentication. Below code snippet demonstrates how these two
    // properties
    // can be set.

    // final Map<String, String> properties = ImmutableMap.of(
    // "routing-tag", "test-routing-tag",
    // "routing-queue", "Low Cost User Queries");
    // final CallHeaders callHeaders = new FlightCallHeaders();
    // properties.forEach(callHeaders::insert);
    // final HeaderCallOption routingCallOptions = new
    // HeaderCallOption(callHeaders);
    // callOptions.add(routingCallOptions);

    // If provided, add client properties to CallOptions.
    if (clientProperties != null) {
      callOptions.add(clientProperties);
    }

    // Perform handshake with the Dremio Flight Server Endpoint.
    client.handshake(callOptions.toArray(new CallOption[callOptions.size()]));

    // Authentication is successful, extract the bearer token returned by the server
    // from the
    // ClientIncomingAuthHeaderMiddleware.Factory. The CredentialCallOption can be
    // used in
    // subsequent Flight RPC requests for bearer token authentication.
    return factory.getCredentialCallOption();
  }

  /**
   * Make a FlightRPC getInfo request with the given query and client properties.
   *
   * @param query   the query to retrieve FlightInfo for.
   * @param options the client properties to execute this request with.
   * @return a FlightInfo object.
   */
  public FlightInfo getFlightInfo(String query, CallOption... options) {
    return client.getInfo(FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8)), options);
  }

  /**
   * Make a FlightRPC getStream request based on the provided FlightInfo object.
   * Retrieves result of the query previously prepared with getInfo.
   *
   * @param flightInfo the FlightInfo object encapsulating information for the
   *                   server to identify the prepared statement with.
   * @param options    the client properties to execute this request with.
   * @return a stream of results.
   */
  public FlightStream getStream(FlightInfo flightInfo, CallOption... options) {
    return client.getStream(flightInfo.getEndpoints().get(0).getTicket(), options);
  }

  /**
   * Make FlightRPC requests to the Dremio Flight Server Endpoint to retrieve
   * results of the provided SQL query.
   *
   * @param query            the SQL query to execute.
   * @param headerCallOption client properties to execute provided SQL query with.
   * @param fileToSaveTo     the file to which the binary data of the resulting
   *                         {@link VectorSchemaRoot} should be saved.
   * @param printToConsole   whether or not the query results should be printed to
   *                         the console.
   * @throws Exception if an error occurs during query execution.
   */
  public void runQuery(String query, HeaderCallOption headerCallOption, File fileToSaveTo, boolean printToConsole) {

    final FlightInfo flightInfo = getFlightInfo(query, bearerToken, headerCallOption);
    LOGGER.info("FlightInfo records {}", flightInfo.getRecords());
    String schema = flightInfo.getSchema().toJson();
    LOGGER.info("FlightInfo schema: {}", schema);

    try (FlightStream stream = getStream(flightInfo, bearerToken, 
      headerCallOption)) {
      if (!stream.next()) {
        LOGGER.warn("Stream had no batches.");
        return;
      }
      int batchNumber = 1;
      try (VectorSchemaRoot root = stream.getRoot()) {
        if (printToConsole) {
          boolean hasMore = true;
          int totalRows = 0;
          while (hasMore) {
            LOGGER.debug("===================Query results");
            LOGGER.debug(root.contentToTSVString());
            int rowCount = root.getRowCount();
            LOGGER.info("Retrieved: {} rows", rowCount);
            hasMore = stream.next();
            LOGGER.info("Query result batch#{} rowCount={}", batchNumber, rowCount);
            totalRows = totalRows + rowCount;
            if (hasMore)
              batchNumber++;
          }
          LOGGER.info("Query returned {} total batches and a total of {} rows ", batchNumber, totalRows);
        }
        if (fileToSaveTo != null) {
          // Need stream to load root!!

          QueryUtils.writeToBinaryFile(root, stream, fileToSaveTo);
          QueryUtils.printFromBinaryFile(fileToSaveTo);
        }
      }
    } catch (Exception ex) {

    }
  }

  /**
   * Make FlightRPC requests to the Dremio Flight Server Endpoint to retrieve
   * results of the provided SQL query.
   *
   * @param query            the SQL query to execute.
   * @param headerCallOption client properties to execute provided SQL query with.
   * @param fileToSaveTo     the file to which the binary data of the resulting
   *                         {@link VectorSchemaRoot} should be saved.
   * @throws Exception if an error occurs during query execution.
   */
  public void runQuery(String query, HeaderCallOption headerCallOption, File fileToSaveTo) {
    runQuery(query, headerCallOption, fileToSaveTo, false);
  }

  /**
   * Make FlightRPC requests to the Dremio Flight Server Endpoint to retrieve
   * results of the provided SQL query.
   *
   * @param query            the SQL query to execute.
   * @param headerCallOption client properties to execute provided SQL query with.
   * @param printToConsole   whether or not the query results should be printed to
   *                         the console.
   * @throws Exception if an error occurs during query execution.
   */
  public void runQuery(String query, HeaderCallOption headerCallOption, boolean printToConsole) {
    runQuery(query, headerCallOption, null, printToConsole);
  }

  /**
   * Make FlightRPC requests to the Dremio Flight Server Endpoint to retrieve
   * results of the provided SQL query.
   *
   * @param query          the SQL query to execute.
   * @param fileToSaveTo   the file to which the binary data of the resulting
   *                       {@link VectorSchemaRoot} should be saved.
   * @param printToConsole whether or not the query results should be printed to
   *                       the console.
   * @throws Exception if an error occurs during query execution.
   */
  public void runQuery(String query, File fileToSaveTo, boolean printToConsole) {
    runQuery(query, null, fileToSaveTo, printToConsole);
  }

  /**
   * Make FlightRPC requests to the Dremio Flight Server Endpoint to retrieve
   * results of the provided SQL query.
   *
   * @param query        the SQL query to execute.
   * @param fileToSaveTo the file to which the binary data of the resulting
   *                     {@link VectorSchemaRoot} should be saved.
   * @throws Exception if an error occurs during query execution.
   */
  public void runQuery(String query, File fileToSaveTo) {
    runQuery(query, null, fileToSaveTo, false);
  }

  /**
   * Make FlightRPC requests to the Dremio Flight Server Endpoint to retrieve
   * results of the provided SQL query.
   *
   * @param query          the SQL query to execute.
   * @param printToConsole whether or not the query results should be printed to
   *                       the console.
   * @throws Exception if an error occurs during query execution.
   */
  public void runQuery(String query, boolean printToConsole)  {
    runQuery(query, null, null, printToConsole);
  }

  /**
   * Make FlightRPC requests to the Dremio Flight Server Endpoint to retrieve
   * results of the provided SQL query.
   *
   * @param query            the SQL query to execute.
   * @param headerCallOption client properties to execute provided SQL query with.
   * @throws Exception if an error occurs during query execution.
   */
  public void runQuery(String query, HeaderCallOption headerCallOption) {
    runQuery(query, headerCallOption, false);
  }

  /**
   * Make FlightRPC requests to the Dremio Flight Server Endpoint to retrieve
   * results of the provided SQL query.
   *
   * @param query the SQL query to execute.
   * @throws Exception if an error occurs during query execution.
   */
  public void runQuery(String query) {
    runQuery(query, false);
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (InterruptedException ex) {
      LOGGER.error("Exception on close",ex);
    }
  }
}
