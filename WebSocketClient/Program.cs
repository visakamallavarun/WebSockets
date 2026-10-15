using System.Net.WebSockets;
using System.Text;

// ---------------------------------------------------------------------------
//  APPLICATION A : the WebSocket CLIENT (uses ClientWebSocket)
//
//  Role reminder:
//    - The CLIENT *initiates*. It reaches out to the server first.
//    - ClientWebSocket is the CLIENT-SIDE api only. It is NOT a server.
//
//  This client proves genuine TWO-WAY communication:
//    1. A RECEIVE LOOP continuously reads whatever the server sends
//       (both the replies AND the unprompted "Server notification" pushes).
//    2. A SEND LOOP reads what you type and sends it — at the SAME TIME,
//       independently of what is being received.
//
//  Lifecycle you are learning:  Create -> Connect -> Send/Receive -> Close -> Dispose
// ---------------------------------------------------------------------------

var serverUri = new Uri("ws://localhost:5170/ws");

// CREATE. 'using' guarantees Dispose() runs at the end to free the socket.
using var client = new ClientWebSocket();

// A cancellation token lets us shut both loops down cleanly (e.g. on Ctrl+C
// or when the user types "exit").
using var cts = new CancellationTokenSource();

try
{
    // CONNECT. This performs the HTTP Upgrade handshake under the hood and
    // waits (without blocking a thread) until the server accepts.
    Console.WriteLine($"[CLIENT] Connecting to {serverUri} ...");
    await client.ConnectAsync(serverUri, cts.Token);
    Console.WriteLine("[CLIENT] Connected. Type a message and press Enter. Type 'exit' to quit.");

    // Start receiving and sending CONCURRENTLY on the same connection.
    Task receiveLoop = ReceiveLoopAsync(client, cts.Token);
    Task sendLoop = SendLoopAsync(client, cts);

    // Wait for whichever finishes first to drive shutdown, then wait for both.
    await Task.WhenAll(receiveLoop, sendLoop);
}
catch (WebSocketException ex)
{
    Console.WriteLine($"[CLIENT] WebSocket error: {ex.Message}");
}
catch (OperationCanceledException)
{
    Console.WriteLine("[CLIENT] Cancelled.");
}
finally
{
    Console.WriteLine("[CLIENT] Done. Socket disposed.");
}

// ---------------------------------------------------------------------------
//  RECEIVE LOOP : keep reading until the server (or we) close the connection.
//  A single receive loop per connection is the standard pattern — you must
//  NOT call ReceiveAsync from two places at once on the same socket.
// ---------------------------------------------------------------------------
static async Task ReceiveLoopAsync(ClientWebSocket client, CancellationToken token)
{
    var buffer = new byte[4 * 1024];

    try
    {
        while (client.State == WebSocketState.Open && !token.IsCancellationRequested)
        {
            WebSocketReceiveResult result =
                await client.ReceiveAsync(buffer, token);

            // Server initiated a close.
            if (result.MessageType == WebSocketMessageType.Close)
            {
                Console.WriteLine("[CLIENT] Server is closing the connection.");
                await client.CloseOutputAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Acknowledging close",
                    CancellationToken.None);
                break;
            }

            string text = Encoding.UTF8.GetString(buffer, 0, result.Count);
            Console.WriteLine($"[CLIENT] <-- {text}");
        }
    }
    catch (OperationCanceledException)
    {
        // Expected on shutdown.
    }
    catch (WebSocketException ex)
    {
        Console.WriteLine($"[CLIENT] Receive error: {ex.Message}");
    }
}

// ---------------------------------------------------------------------------
//  SEND LOOP : read console input and send it. Runs independently of receiving.
// ---------------------------------------------------------------------------
static async Task SendLoopAsync(ClientWebSocket client, CancellationTokenSource cts)
{
    try
    {
        while (!cts.IsCancellationRequested && client.State == WebSocketState.Open)
        {
            // Console.ReadLine blocks the calling thread, so push it to a
            // background thread-pool thread to avoid stalling the receive loop.
            string? line = await Task.Run(Console.ReadLine);

            if (line is null || line.Equals("exit", StringComparison.OrdinalIgnoreCase))
            {
                // CLOSE politely: send a close frame and stop everything.
                await client.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Client exiting",
                    CancellationToken.None);
                cts.Cancel();
                break;
            }

            byte[] bytes = Encoding.UTF8.GetBytes(line);
            await client.SendAsync(
                new ArraySegment<byte>(bytes),
                WebSocketMessageType.Text,
                endOfMessage: true,
                cts.Token);
            Console.WriteLine($"[CLIENT] --> {line}");
        }
    }
    catch (OperationCanceledException)
    {
        // Expected on shutdown.
    }
    catch (WebSocketException ex)
    {
        Console.WriteLine($"[CLIENT] Send error: {ex.Message}");
        cts.Cancel();
    }
}
