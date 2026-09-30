using System;
using System.Threading.Tasks;
using Centrifugal.Centrifuge;
using Xunit;

namespace Centrifugal.Centrifuge.Tests
{
    /// <summary>
    /// Tests for CentrifugeClient.
    /// </summary>
    public class ClientTests
    {
        /// <summary>An endpoint no transport could connect to is a configuration error at construction,
        /// not endless connect attempts.</summary>
        [Theory]
        [InlineData("")]
        [InlineData("ws://")]
        [InlineData("http://[")]
        [InlineData("ws://exa mple.com/connection/websocket")]
        [InlineData("http://host:99999/connection/http_stream")]
        [InlineData("/connection/websocket")]
        [InlineData("ftp://host/connection")]
        public void Client_Constructor_ThrowsOnMalformedEndpoint(string endpoint)
        {
            Assert.Throws<ArgumentException>(() => new CentrifugeClient(endpoint));
            Assert.Throws<ArgumentException>(() => new CentrifugeClient(new[]
            {
                new CentrifugeTransportEndpoint(CentrifugeTransportType.WebSocket, "ws://localhost:8000/connection/websocket"),
                new CentrifugeTransportEndpoint(CentrifugeTransportType.HttpStream, endpoint),
            }));
        }

        /// <summary>A native HTTP stream can't send to such an emulation endpoint: every command after
        /// connect would fail and reconnect the session. A WebSocket-only client doesn't use it.</summary>
        [Theory]
        [InlineData("/emulation")]
        [InlineData("ws://localhost:8000/emulation")]
        [InlineData("http://[")]
        public void Client_Constructor_ThrowsOnMalformedEmulationEndpoint(string emulationEndpoint)
        {
            var options = new CentrifugeClientOptions { EmulationEndpoint = emulationEndpoint };

            Assert.Throws<CentrifugeConfigurationException>(
                () => new CentrifugeClient("http://localhost:8000/connection/http_stream", options));
            using var webSocketOnly = new CentrifugeClient("ws://localhost:8000/connection/websocket", options);
        }

        /// <summary>The default emulation endpoint is the root-level /emulation of the endpoint's host; a
        /// relative (browser) endpoint keeps it relative to the page — outside Windows its rooted path
        /// parses as an absolute file: URI.</summary>
        [Theory]
        [InlineData("/connection/http_stream", "/emulation")]
        [InlineData("https://host:8443/prefix/connection/http_stream", "https://host:8443/emulation")]
        public void EmulationEndpoint_DefaultsToRootOfEndpointHost(string endpoint, string expected)
        {
            using var client = new CentrifugeClient("ws://localhost:8000/connection/websocket");
            var emulationEndpointFor = typeof(CentrifugeClient).GetMethod(
                "EmulationEndpointFor", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;

            Assert.Equal(expected, emulationEndpointFor.Invoke(client, new object[] { endpoint }));
        }

        [Fact]
        public void Client_Constructor_ThrowsOnFallbackEndpointOfAnotherTransport()
        {
            Assert.Throws<ArgumentException>(() => new CentrifugeClient(new[]
            {
                new CentrifugeTransportEndpoint(CentrifugeTransportType.HttpStream, "ws://localhost:8000/connection/websocket"),
            }));
        }

        [Fact]
        public void Client_Constructor_ThrowsOnNullEndpoint()
        {
            Assert.Throws<ArgumentNullException>(() => new CentrifugeClient((string)null!));
        }

        [Fact]
        public void Client_InitialState_IsDisconnected()
        {
            var client = new CentrifugeClient("ws://localhost:8000/connection/websocket");
            Assert.Equal(CentrifugeClientState.Disconnected, client.State);
        }

        [Fact]
        public async Task Client_Connect_ChangesStateToConnecting()
        {
            var client = new CentrifugeClient("ws://localhost:8000/connection/websocket");
            var stateChangedTcs = new TaskCompletionSource<bool>();

            client.StateChanged += (sender, args) =>
            {
                if (args.NewState == CentrifugeClientState.Connecting)
                {
                    stateChangedTcs.TrySetResult(true);
                }
            };

            // This will fail to connect since there's no server, but state should change
            client.Connect();

            // Wait for state change event with timeout
            var stateChanged = await stateChangedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

            Assert.True(stateChanged);
        }

        [Fact]
        public void Client_NewSubscription_CreatesSubscription()
        {
            var client = new CentrifugeClient("ws://localhost:8000/connection/websocket");
            var sub = client.NewSubscription("test");

            Assert.NotNull(sub);
            Assert.Equal("test", sub.Channel);
            Assert.Equal(CentrifugeSubscriptionState.Unsubscribed, sub.State);
        }

        [Fact]
        public void Client_NewSubscription_ThrowsOnDuplicate()
        {
            var client = new CentrifugeClient("ws://localhost:8000/connection/websocket");
            client.NewSubscription("test");

            var ex = Assert.Throws<CentrifugeDuplicateSubscriptionException>(() => client.NewSubscription("test"));
            Assert.Equal("test", ex.Channel);
            Assert.Equal(0, ex.Code);
            Assert.False(ex.Temporary);
        }

        [Fact]
        public void Client_GetSubscription_ReturnsNullForNonexistent()
        {
            var client = new CentrifugeClient("ws://localhost:8000/connection/websocket");
            var sub = client.GetSubscription("nonexistent");

            Assert.Null(sub);
        }

        [Fact]
        public void Client_GetSubscription_ReturnsExisting()
        {
            var client = new CentrifugeClient("ws://localhost:8000/connection/websocket");
            var created = client.NewSubscription("test");
            var retrieved = client.GetSubscription("test");

            Assert.Same(created, retrieved);
        }

        [Fact]
        public void Client_Disconnect_ChangesStateToDisconnected()
        {
            var client = new CentrifugeClient("ws://localhost:8000/connection/websocket");
            client.Disconnect();

            Assert.Equal(CentrifugeClientState.Disconnected, client.State);
        }

        // Integration tests require a running Centrifugo server
        // These would be similar to the JavaScript tests in centrifuge.test.ts
        //
        // Example structure for integration tests:
        //
        // [Fact]
        // public async Task Client_ConnectsAndDisconnects()
        // {
        //     var client = new CentrifugeClient("ws://localhost:8000/connection/websocket");
        //     var connectedEvent = new TaskCompletionSource<bool>();
        //     var disconnectedEvent = new TaskCompletionSource<bool>();
        //
        //     client.Connected += (s, e) => connectedEvent.SetResult(true);
        //     client.Disconnected += (s, e) => disconnectedEvent.SetResult(true);
        //
        //     client.Connect(); await client.ReadyAsync();
        //     await connectedEvent.Task.WithTimeout(TimeSpan.FromSeconds(5));
        //     Assert.Equal(CentrifugeClientState.Connected, client.State);
        //
        //     client.Disconnect();
        //     await disconnectedEvent.Task.WithTimeout(TimeSpan.FromSeconds(5));
        //     Assert.Equal(CentrifugeClientState.Disconnected, client.State);
        // }
    }

    /// <summary>
    /// Tests for options validation.
    /// </summary>
    public class OptionsTests
    {
        [Fact]
        public void ClientOptions_Validate_ThrowsOnNegativeMinDelay()
        {
            var options = new CentrifugeClientOptions
            {
                MinReconnectDelay = TimeSpan.FromMilliseconds(-1)
            };

            Assert.Throws<CentrifugeConfigurationException>(() => options.Validate());
        }

        [Fact]
        public void ClientOptions_Validate_ThrowsOnMaxLessThanMin()
        {
            var options = new CentrifugeClientOptions
            {
                MinReconnectDelay = TimeSpan.FromMilliseconds(1000),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(500)
            };

            Assert.Throws<CentrifugeConfigurationException>(() => options.Validate());
        }

        [Fact]
        public void ClientOptions_Validate_ThrowsOnNegativeTimeout()
        {
            var options = new CentrifugeClientOptions
            {
                Timeout = TimeSpan.FromMilliseconds(-1)
            };

            Assert.Throws<CentrifugeConfigurationException>(() => options.Validate());
        }

        [Theory]
        [InlineData(nameof(CentrifugeClientOptions.Timeout))]
        [InlineData(nameof(CentrifugeClientOptions.OpenTimeout))]
        [InlineData(nameof(CentrifugeClientOptions.MaxReconnectDelay))]
        public void ClientOptions_Validate_ThrowsOnIntervalBeyondTimerRange(string option)
        {
            var options = new CentrifugeClientOptions();
            typeof(CentrifugeClientOptions).GetProperty(option)!.SetValue(options, TimeSpan.FromDays(30));

            Assert.Throws<CentrifugeConfigurationException>(() => options.Validate());
        }

        [Fact]
        public void CentrifugeSubscriptionOptions_Validate_ThrowsOnNegativeMinDelay()
        {
            var options = new CentrifugeSubscriptionOptions
            {
                MinResubscribeDelay = TimeSpan.FromMilliseconds(-1)
            };

            Assert.Throws<CentrifugeConfigurationException>(() => options.Validate());
        }
    }
}
