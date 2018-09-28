/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io._
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.{HashMap, Random}

import javax.net.ssl._
import com.yammer.metrics.core.Gauge
import com.yammer.metrics.{Metrics => YammerMetrics}
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.{AbstractRequest, ProduceRequest, RequestHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit._
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SocketServerTest extends JUnitSuite {
  val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
  props.put("listeners", "PLAINTEXT://localhost:0")
  props.put("num.network.threads", "1")
  props.put("socket.send.buffer.bytes", "300000")
  props.put("socket.receive.buffer.bytes", "300000")
  props.put("queued.max.requests", "50")
  props.put("socket.request.max.bytes", "50")
  props.put("max.connections.per.ip", "5")
  props.put("connections.max.idle.ms", "60000")
  val config = KafkaConfig.fromProps(props)
  val metrics = new Metrics
  val credentialProvider = new CredentialProvider(config.saslEnabledMechanisms)
  val localAddress = InetAddress.getLoopbackAddress

  // Clean-up any metrics left around by previous tests
  for (metricName <- YammerMetrics.defaultRegistry.allMetrics.keySet.asScala)
    YammerMetrics.defaultRegistry.removeMetric(metricName)

  val server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider)
  server.startup()
  val sockets = new ArrayBuffer[Socket]

  def sendRequest(socket: Socket, request: Array[Byte], id: Option[Short] = None, flush: Boolean = true) {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    id match {
      case Some(id) =>
        outgoing.writeInt(request.length + 2)
        outgoing.writeShort(id)
      case None =>
        outgoing.writeInt(request.length)
    }
    outgoing.write(request)
    if (flush)
      outgoing.flush()
  }

  def receiveResponse(socket: Socket): Array[Byte] = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    response
  }

  private def receiveRequest(channel: RequestChannel, timeout: Long = 2000L): RequestChannel.Request = {
    val request = channel.receiveRequest(timeout)
    assertNotNull("receiveRequest timed out", request)
    request
  }

  /* A simple request handler that just echos back the response */
  def processRequest(channel: RequestChannel) {
    val request = receiveRequest(channel)
    processRequest(channel, request)
  }

  def processRequest(channel: RequestChannel, request: RequestChannel.Request) {
    val byteBuffer = request.body[AbstractRequest].serialize(request.header)
    byteBuffer.rewind()

    val send = new NetworkSend(request.connectionId, byteBuffer)
    channel.sendResponse(RequestChannel.Response(request, send))
  }

  def connect(s: SocketServer = server, protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT) = {
    val socket = new Socket("localhost", s.boundPort(ListenerName.forSecurityProtocol(protocol)))
    sockets += socket
    socket
  }

  @After
  def tearDown() {
    metrics.close()
    server.shutdown()
    sockets.foreach(_.close())
    sockets.clear()
  }

  private def producerRequestBytes(ack: Short = 0): Array[Byte] = {
    val apiKey: Short = 0
    val correlationId = -1
    val clientId = ""
    val ackTimeoutMs = 10000

    val emptyRequest = new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE, ack, ackTimeoutMs,
      new HashMap[TopicPartition, MemoryRecords]()).build()
    val emptyHeader = new RequestHeader(apiKey, emptyRequest.version, clientId, correlationId)
    val byteBuffer = emptyRequest.serialize(emptyHeader)
    byteBuffer.rewind()

    val serializedBytes = new Array[Byte](byteBuffer.remaining)
    byteBuffer.get(serializedBytes)
    serializedBytes
  }

  private def sendRequestsUntilStagedReceive(server: SocketServer, socket: Socket, requestBytes: Array[Byte]): RequestChannel.Request = {
    def sendTwoRequestsReceiveOne(): RequestChannel.Request = {
      sendRequest(socket, requestBytes, flush = false)
      sendRequest(socket, requestBytes, flush = true)
      receiveRequest(server.requestChannel)
    }
    val (request, hasStagedReceives) = TestUtils.computeUntilTrue(sendTwoRequestsReceiveOne()) { req =>
      val connectionId = req.connectionId
      val hasStagedReceives = server.processor(0).numStagedReceives(connectionId) > 0
      if (!hasStagedReceives) {
        processRequest(server.requestChannel, req)
        processRequest(server.requestChannel)
      }
      hasStagedReceives
    }
    assertTrue(s"Receives not staged for ${org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS} ms", hasStagedReceives)
    request
  }

  @Test
  def simpleRequest() {
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    val serializedBytes = producerRequestBytes()

    // Test PLAINTEXT socket
    sendRequest(plainSocket, serializedBytes)
    processRequest(server.requestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(plainSocket).toSeq)
  }

  @Test
  def tooBigRequestIsRejected() {
    val tooManyBytes = new Array[Byte](server.config.socketRequestMaxBytes + 1)
    new Random().nextBytes(tooManyBytes)
    val socket = connect()
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(tooManyBytes.length)
    try {
      // Server closes client connection when it processes the request length because
      // it is too big. The write of request body may fail if the connection has been closed.
      outgoing.write(tooManyBytes)
      outgoing.flush()
      receiveResponse(socket)
    } catch {
      case _: IOException => // thats fine
    }
  }

  @Test
  def testGracefulClose() {
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    val serializedBytes = producerRequestBytes()

    for (_ <- 0 until 10)
      sendRequest(plainSocket, serializedBytes)
    plainSocket.close()
    for (_ <- 0 until 10) {
      val request = server.requestChannel.receiveRequest(2000)
      assertNotNull("receiveRequest timed out", request)
      server.requestChannel.sendResponse(RequestChannel.Response(request, None, RequestChannel.NoOpAction))
    }
  }

  @Test
  def testSocketsCloseOnShutdown() {
    // open a connection
    val plainSocket = connect(protocol = SecurityProtocol.PLAINTEXT)
    plainSocket.setTcpNoDelay(true)
    val bytes = new Array[Byte](40)
    // send a request first to make sure the connection has been picked up by the socket server
    sendRequest(plainSocket, bytes, Some(0))
    processRequest(server.requestChannel)
    // the following sleep is necessary to reliably detect the connection close when we send data below
    Thread.sleep(200L)
    // make sure the sockets are open
    server.acceptors.values.map(acceptor => assertFalse(acceptor.serverChannel.socket.isClosed))
    // then shutdown the server
    server.shutdown()

    val largeChunkOfBytes = new Array[Byte](1000000)
    // doing a subsequent send should throw an exception as the connection should be closed.
    // send a large chunk of bytes to trigger a socket flush
    try {
      sendRequest(plainSocket, largeChunkOfBytes, Some(0))
      fail("expected exception when writing to closed plain socket")
    } catch {
      case _: IOException => // expected
    }
  }

  @Test
  def testMaxConnectionsPerIp() {
    // make the maximum allowable number of connections
    val conns = (0 until server.config.maxConnectionsPerIp).map(_ => connect())
    // now try one more (should fail)
    val conn = connect()
    conn.setSoTimeout(3000)
    assertEquals(-1, conn.getInputStream().read())
    conn.close()

    // it should succeed after closing one connection
    val address = conns.head.getInetAddress
    conns.head.close()
    TestUtils.waitUntilTrue(() => server.connectionCount(address) < conns.length,
      "Failed to decrement connection count after close")
    val conn2 = connect()
    val serializedBytes = producerRequestBytes()
    sendRequest(conn2, serializedBytes)
    val request = server.requestChannel.receiveRequest(2000)
    assertNotNull(request)
  }

  @Test
  def testMaxConnectionsPerIpOverrides() {
    val overrideNum = server.config.maxConnectionsPerIp + 1
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    overrideProps.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, s"localhost:$overrideNum")
    val serverMetrics = new Metrics()
    val overrideServer = new SocketServer(KafkaConfig.fromProps(overrideProps), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      // make the maximum allowable number of connections
      val conns = (0 until overrideNum).map(_ => connect(overrideServer))

      // it should succeed
      val serializedBytes = producerRequestBytes()
      sendRequest(conns.last, serializedBytes)
      val request = overrideServer.requestChannel.receiveRequest(2000)
      assertNotNull(request)

      // now try one more (should fail)
      val conn = connect(overrideServer)
      conn.setSoTimeout(3000)
      assertEquals(-1, conn.getInputStream.read())
    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  @Test
  def testSslSocketServer() {
    val trustStoreFile = File.createTempFile("truststore", ".jks")
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, interBrokerSecurityProtocol = Some(SecurityProtocol.SSL),
      trustStoreFile = Some(trustStoreFile))
    overrideProps.put(KafkaConfig.ListenersProp, "SSL://localhost:0")

    val serverMetrics = new Metrics
    val overrideServer = new SocketServer(KafkaConfig.fromProps(overrideProps), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      val sslContext = SSLContext.getInstance("TLSv1.2")
      sslContext.init(null, Array(TestUtils.trustAllCerts), new java.security.SecureRandom())
      val socketFactory = sslContext.getSocketFactory
      val sslSocket = socketFactory.createSocket("localhost",
        overrideServer.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SSL))).asInstanceOf[SSLSocket]
      sslSocket.setNeedClientAuth(false)

      val apiKey = ApiKeys.PRODUCE.id
      val correlationId = -1
      val clientId = ""
      val ackTimeoutMs = 10000
      val ack = 0: Short
      val emptyRequest = new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE, ack, ackTimeoutMs,
        new HashMap[TopicPartition, MemoryRecords]()).build()
      val emptyHeader = new RequestHeader(apiKey, emptyRequest.version, clientId, correlationId)

      val byteBuffer = emptyRequest.serialize(emptyHeader)
      byteBuffer.rewind()
      val serializedBytes = new Array[Byte](byteBuffer.remaining)
      byteBuffer.get(serializedBytes)

      sendRequest(sslSocket, serializedBytes)
      processRequest(overrideServer.requestChannel)
      assertEquals(serializedBytes.toSeq, receiveResponse(sslSocket).toSeq)
      sslSocket.close()
    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  @Test
  def testSessionPrincipal() {
    val socket = connect()
    val bytes = new Array[Byte](40)
    sendRequest(socket, bytes, Some(0))
    assertEquals(KafkaPrincipal.ANONYMOUS, server.requestChannel.receiveRequest(2000).session.principal)
  }

  /* Test that we update request metrics if the client closes the connection while the broker response is in flight. */
  @Test
  def testClientDisconnectionUpdatesRequestMetrics() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider) {
      override def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, requestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, listenerName, protocol, config, metrics, credentialProvider) {
          override protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send) {
            conn.close()
            super.sendResponse(response, responseSend)
          }
        }
      }
    }
    try {
      overrideServer.startup()
      conn = connect(overrideServer)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn, serializedBytes)

      val channel = overrideServer.requestChannel
      val request = channel.receiveRequest(2000)

      val requestMetrics = RequestMetrics.metricsMap(ApiKeys.forId(request.requestId).name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val expectedTotalTimeCount = totalTimeHistCount() + 1

      // send a large buffer to ensure that the broker detects the client disconnection while writing to the socket channel.
      // On Mac OS X, the initial write seems to always succeed and it is able to write up to 102400 bytes on the initial
      // write. If the buffer is smaller than this, the write is considered complete and the disconnection is not
      // detected. If the buffer is larger than 102400 bytes, a second write is attempted and it fails with an
      // IOException.
      val send = new NetworkSend(request.connectionId, ByteBuffer.allocate(550000))
      channel.sendResponse(RequestChannel.Response(request, send))
      TestUtils.waitUntilTrue(() => totalTimeHistCount() == expectedTotalTimeCount,
        s"request metrics not updated, expected: $expectedTotalTimeCount, actual: ${totalTimeHistCount()}")

    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  @Test
  def testClientDisconnectionWithStagedReceivesFullyProcessed() {
    val socket = connect(server)

    // Setup channel to client with staged receives so when client disconnects
    // it will be stored in Selector.closingChannels
    val serializedBytes = producerRequestBytes(1)
    val request = sendRequestsUntilStagedReceive(server, socket, serializedBytes)
    val connectionId = request.connectionId

    // Set SoLinger to 0 to force a hard disconnect via TCP RST
    socket.setSoLinger(true, 0)
    socket.close()

    // Complete request with socket exception so that the channel is removed from Selector.closingChannels
    processRequest(server.requestChannel, request)
    TestUtils.waitUntilTrue(() => server.processor(0).openOrClosingChannel(connectionId).isEmpty,
      "Channel not closed after failed send")
  }

  /*
   * Test that we update request metrics if the channel has been removed from the selector when the broker calls
   * `selector.send` (selector closes old connections, for example).
   */
  @Test
  def testBrokerSendAfterChannelClosedUpdatesRequestMetrics() {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    props.setProperty(KafkaConfig.ConnectionsMaxIdleMsProp, "100")
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      conn = connect(overrideServer)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn, serializedBytes)
      val channel = overrideServer.requestChannel
      val request = channel.receiveRequest(2000)

      TestUtils.waitUntilTrue(() => overrideServer.processor(request.processor).channel(request.connectionId).isEmpty,
        s"Idle connection `${request.connectionId}` was not closed by selector")

      val requestMetrics = RequestMetrics.metricsMap(ApiKeys.forId(request.requestId).name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val expectedTotalTimeCount = totalTimeHistCount() + 1

      processRequest(channel, request)

      TestUtils.waitUntilTrue(() => totalTimeHistCount() == expectedTotalTimeCount,
        s"request metrics not updated, expected: $expectedTotalTimeCount, actual: ${totalTimeHistCount()}")

    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }

  }

  @Test
  def testMetricCollectionAfterShutdown(): Unit = {
    server.shutdown()

    val nonZeroMetricNamesAndValues = YammerMetrics
      .defaultRegistry
      .allMetrics.asScala
      .filterKeys(k => k.getName.endsWith("IdlePercent") || k.getName.endsWith("NetworkProcessorAvgIdlePercent"))
      .collect { case (k, metric: Gauge[_]) => (k, metric.value().asInstanceOf[Double]) }
      .filter { case (_, value) => value != 0.0 }

    assertEquals(Map.empty, nonZeroMetricNamesAndValues)
  }

  @Test
  def testProcessorMetricsTags(): Unit = {
    val kafkaMetricNames = metrics.metrics.keySet.asScala.filter(_.tags.asScala.get("listener").nonEmpty)
    assertFalse(kafkaMetricNames.isEmpty)

    val expectedListeners = Set("PLAINTEXT", "TRACE")
    kafkaMetricNames.foreach { kafkaMetricName =>
      assertTrue(expectedListeners.contains(kafkaMetricName.tags.get("listener")))
    }

    // legacy metrics not tagged
    val yammerMetricsNames = YammerMetrics.defaultRegistry.allMetrics.asScala
      .filterKeys(_.getType.equals("Processor"))
      .collect { case (k, _: Gauge[_]) => k }
    assertFalse(yammerMetricsNames.isEmpty)

    yammerMetricsNames.foreach { yammerMetricName =>
      assertFalse(yammerMetricName.getMBeanName.contains("listener="))
    }
  }

  @Test
  def testConnectionRateLimit(): Unit = {
    val numConnections = 10
    props.put("max.connections.per.ip", numConnections.toString)
    val testableServer = new TestableSocketServer(1)
    testableServer.startup()
    try {
      val testableSelector = testableServer.testableSelector
      testableSelector.pollBlockMs = 100 // To ensure that Processor is blocked
      testableSelector.operationCounts.clear()
      val sockets = (1 to numConnections).map(_ => connect(testableServer))
      testableSelector.waitForOperations(SelectorOperation.Register, numConnections)
      val pollCount = testableSelector.operationCounts(SelectorOperation.Poll)
      assertTrue(s"Connections created too quickly: $pollCount", pollCount >= numConnections)
      assertProcessorHealthy(testableServer, sockets)
    } finally {
      shutdownServerAndMetrics(testableServer)
    }
  }

  def shutdownServerAndMetrics(server: SocketServer): Unit = {
    server.shutdown()
    server.metrics.close()
  }

  // Create a client connection, process one request and return (client socket, connectionId)
  def connectAndProcessRequest(s: SocketServer): (Socket, String) = {
    val socket = connect(s)
    val request = sendAndReceiveRequest(socket, s)
    processRequest(s.requestChannel, request)
    (socket, request.connectionId)
  }

  def sendAndReceiveRequest(socket: Socket, server: SocketServer): RequestChannel.Request = {
    sendRequest(socket, producerRequestBytes())
    receiveRequest(server.requestChannel)
  }

  private def assertProcessorHealthy(testableServer: TestableSocketServer, healthySockets: Seq[Socket] = Seq.empty): Unit = {
    val selector = testableServer.testableSelector
    selector.reset()
    val requestChannel = testableServer.requestChannel

    // Check that existing channels behave as expected
    healthySockets.foreach { socket =>
      val request = sendAndReceiveRequest(socket, testableServer)
      processRequest(requestChannel, request)
      socket.close()
                           }
    TestUtils.waitUntilTrue(() => testableServer.connectionCount(localAddress) == 0, "Channels not removed")

    // Check new channel behaves as expected
    val (socket, connectionId) = connectAndProcessRequest(testableServer)
    assertArrayEquals(producerRequestBytes(), receiveResponse(socket))
    assertNotNull("Channel should not have been closed", selector.channel(connectionId))
    assertNull("Channel should not be closing", selector.closingChannel(connectionId))
    socket.close()
    TestUtils.waitUntilTrue(() => testableServer.connectionCount(localAddress) == 0, "Channels not removed")
  }

  class TestableSocketServer(val connectionQueueSize: Int = 20) extends SocketServer(KafkaConfig.fromProps(props),
                                                                                     new Metrics, Time.SYSTEM, credentialProvider) {

    @volatile var selector: Option[TestableSelector] = None

    override def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                              protocol: SecurityProtocol): Processor = {
      new Processor(id, time, config.socketRequestMaxBytes, requestChannel, connectionQuotas,
                    config.connectionsMaxIdleMs, listenerName, protocol, config, metrics, credentialProvider,
                    connectionQueueSize) {

        override protected[network] def createSelector(channelBuilder: ChannelBuilder): Selector = {
          val testableSelector = new TestableSelector(config, channelBuilder, time, metrics)
          assertEquals(None, selector)
          selector = Some(testableSelector)
          testableSelector
        }
      }
    }

    def testableSelector: TestableSelector =
      selector.getOrElse(throw new IllegalStateException("Selector not created"))

    def waitForChannelClose(connectionId: String, locallyClosed: Boolean): Unit = {
      val selector = testableSelector
      if (locallyClosed) {
        TestUtils.waitUntilTrue(() => selector.allLocallyClosedChannels.contains(connectionId),
                                s"Channel not closed: $connectionId")
        assertTrue("Unexpected disconnect notification", testableSelector.allDisconnectedChannels.isEmpty)
      } else {
        TestUtils.waitUntilTrue(() => selector.allDisconnectedChannels.contains(connectionId),
                                s"Disconnect notification not received: $connectionId")
        assertTrue("Channel closed locally", testableSelector.allLocallyClosedChannels.isEmpty)
      }
      val openCount = selector.allChannels.size - 1 // minus one for the channel just closed above
      TestUtils.waitUntilTrue(() => connectionCount(localAddress) == openCount, "Connection count not decremented")
      TestUtils.waitUntilTrue(() =>
                                processor(0).inflightResponseCount == 0, "Inflight responses not cleared")
      assertNull("Channel not removed", selector.channel(connectionId))
      assertNull("Closing channel not removed", selector.closingChannel(connectionId))
    }
  }

  sealed trait SelectorOperation
  object SelectorOperation {
    case object Register extends SelectorOperation
    case object Poll extends SelectorOperation
    case object Send extends SelectorOperation
    case object Mute extends SelectorOperation
    case object Unmute extends SelectorOperation
    case object Wakeup extends SelectorOperation
    case object Close extends SelectorOperation
    case object CloseSelector extends SelectorOperation
  }

  class TestableSelector(config: KafkaConfig, channelBuilder: ChannelBuilder, time: Time, metrics: Metrics)
    extends Selector(config.socketRequestMaxBytes, config.connectionsMaxIdleMs,
                     metrics, time, "socket-server", new HashMap, false, true, channelBuilder) {

    val failures = mutable.Map[SelectorOperation, Exception]()
    val operationCounts = mutable.Map[SelectorOperation, Int]().withDefaultValue(0)
    val allChannels = mutable.Set[String]()
    val allLocallyClosedChannels = mutable.Set[String]()
    val allDisconnectedChannels = mutable.Set[String]()
    val allFailedChannels = mutable.Set[String]()

    // Enable data from `Selector.poll()` to be deferred to a subsequent poll() until
    // the number of elements of that type reaches `minPerPoll`. This enables tests to verify
    // that failed processing doesn't impact subsequent processing within the same iteration.
    class PollData[T] {
      var minPerPoll = 1
      val deferredValues = mutable.Buffer[T]()
      val currentPollValues = mutable.Buffer[T]()
      def update(newValues: mutable.Buffer[T]): Unit = {
        if (currentPollValues.nonEmpty || deferredValues.size + newValues.size >= minPerPoll) {
          if (deferredValues.nonEmpty) {
            currentPollValues ++= deferredValues
            deferredValues.clear()
          }
          currentPollValues ++= newValues
        } else
          deferredValues ++= newValues
      }
      def reset(): Unit = {
        currentPollValues.clear()
      }
    }
    val cachedCompletedReceives = new PollData[NetworkReceive]()
    val cachedCompletedSends = new PollData[Send]()
    val cachedDisconnected = new PollData[(String, ChannelState)]()
    val allCachedPollData = Seq(cachedCompletedReceives, cachedCompletedSends, cachedDisconnected)
    @volatile var minWakeupCount = 0
    @volatile var pollTimeoutOverride: Option[Long] = None
    @volatile var pollBlockMs = 0

    def addFailure(operation: SelectorOperation, exception: Option[Exception] = None) {
      failures += operation ->
                  exception.getOrElse(new IllegalStateException(s"Test exception during $operation"))
    }

    private def onOperation(operation: SelectorOperation, connectionId: Option[String], onFailure: => Unit): Unit = {
      operationCounts(operation) += 1
      failures.remove(operation).foreach { e =>
        connectionId.foreach(allFailedChannels.add)
        onFailure
        throw e
                                         }
    }

    def waitForOperations(operation: SelectorOperation, minExpectedTotal: Int): Unit = {
      TestUtils.waitUntilTrue(() =>
                                operationCounts.getOrElse(operation, 0) >= minExpectedTotal, "Operations not performed within timeout")
    }

    def runOp[T](operation: SelectorOperation, connectionId: Option[String],
                 onFailure: => Unit = {})(code: => T): T = {
      // If a failure is set on `operation`, throw that exception even if `code` fails
      try code
      finally onOperation(operation, connectionId, onFailure)
    }

    override def register(id: String, socketChannel: SocketChannel): Unit = {
      runOp(SelectorOperation.Register, Some(id), onFailure = close(id)) {
                                                                           super.register(id, socketChannel)
                                                                         }
    }

    override def send(s: Send): Unit = {
      runOp(SelectorOperation.Send, Some(s.destination)) {
                                                           super.send(s)
                                                         }
    }

    override def poll(timeout: Long): Unit = {
      try {
        if (pollBlockMs > 0)
          Thread.sleep(pollBlockMs)
        allCachedPollData.foreach(_.reset)
        runOp(SelectorOperation.Poll, None) {
                                              super.poll(pollTimeoutOverride.getOrElse(timeout))
                                            }
      } finally {
        super.channels.asScala.foreach(allChannels += _.id)
        allDisconnectedChannels ++= super.disconnected.asScala.keys
        cachedCompletedReceives.update(super.completedReceives.asScala)
        cachedCompletedSends.update(super.completedSends.asScala)
        cachedDisconnected.update(super.disconnected.asScala.toBuffer)
      }
    }

    override def mute(id: String): Unit = {
      runOp(SelectorOperation.Mute, Some(id)) {
                                                super.mute(id)
                                              }
    }

    override def unmute(id: String): Unit = {
      runOp(SelectorOperation.Unmute, Some(id)) {
                                                  super.unmute(id)
                                                }
    }

    override def wakeup(): Unit = {
      runOp(SelectorOperation.Wakeup, None) {
                                              if (minWakeupCount > 0)
                                                minWakeupCount -= 1
                                              if (minWakeupCount <= 0)
                                                super.wakeup()
                                            }
    }

    override def disconnected: java.util.Map[String, ChannelState] = cachedDisconnected.currentPollValues.toMap.asJava

    override def completedSends: java.util.List[Send] = cachedCompletedSends.currentPollValues.asJava

    override def completedReceives: java.util.List[NetworkReceive] = cachedCompletedReceives.currentPollValues.asJava

    override def close(id: String): Unit = {
      runOp(SelectorOperation.Close, Some(id)) {
                                                 super.close(id)
                                                 allLocallyClosedChannels += id
                                               }
    }

    override def close(): Unit = {
      runOp(SelectorOperation.CloseSelector, None) {
                                                     super.close()
                                                   }
    }

    def updateMinWakeup(count: Int): Unit = {
      minWakeupCount = count
      // For tests that ignore wakeup to process responses together, increase poll timeout
      // to ensure that poll doesn't complete before the responses are ready
      pollTimeoutOverride = Some(1000L)
      // Wakeup current poll to force new poll timeout to take effect
      super.wakeup()
    }

    def reset(): Unit = {
      failures.clear()
      allCachedPollData.foreach(_.minPerPoll = 1)
    }

    // Since all sockets use the same local host, it is sufficient to check the local port
    def isSocketConnectionId(connectionId: String, socket: Socket): Boolean =
      connectionId.contains(s":${socket.getLocalPort}-")

    def notFailed(sockets: Seq[Socket]): Seq[Socket] = {
      // Each test generates failure for exactly one failed channel
      assertEquals(1, allFailedChannels.size)
      val failedConnectionId = allFailedChannels.head
      sockets.filterNot(socket => isSocketConnectionId(failedConnectionId, socket))
    }
  }
}
