using System.Reflection;
using Centrifugal.Centrifuge.Transports;
using Xunit;

namespace Centrifugal.Centrifuge.Tests
{
    /// <summary>The no-ping timeout of a client's session, fired the way its ping timer does — bound to the transport.</summary>
    internal static class NoPing
    {
        /// <summary>Fires the no-ping timeout of the current transport session; the client must be in one.</summary>
        public static void Fire(CentrifugeClient client)
        {
            var transport = Transport(client);
            Assert.NotNull(transport);
            client.NoPing(transport);
        }

        /// <summary>The client's current transport (null between sessions).</summary>
        public static ITransport? Transport(CentrifugeClient client) =>
            (ITransport?)typeof(CentrifugeClient)
                .GetField("_transport", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(client);
    }
}
