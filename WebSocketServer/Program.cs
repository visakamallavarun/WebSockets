using System.Net.WebSockets;
using System.Text;

// ---------------------------------------------------------------------------
//  APPLICATION B : the WebSocket SERVER (ASP.NET Core)
//
//  Role reminder:
//    - The SERVER *listens* and *waits*. It never reaches out first.
//    - A client connects to it, and AFTER that the server may speak freely.
//
//  This server proves genuine TWO-WAY communication:
//    1. It runs a RECEIVE LOOP that reads whatever the client sends.
//    2. At the SAME TIME it runs a NOTIFY LOOP that pushes messages to the
//       client on a timer, WITHOUT waiting for the client to ask.
//
//  ClientWebSocket (used by Application A) is the client side only.
//  This file is the matching server side.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// Turn on the WebSocket middleware. Without this call, ASP.NET Core will not
// perform the HTTP "Upgrade" handshake that turns a normal HTTP connection
// into a persistent WebSocket connection.
app.UseWebSockets();

// A normal HTTP endpoint, just so you can confirm the server is alive in a
// browser at http://localhost:5170/
app.MapGet("/", () => "WebSocket server is running. Connect a client to ws://localhost:5170/ws");

// The WebSocket endpoint. The client connects to ws://localhost:5170/ws
app.Map("/ws", async context =>
{
    // Reject anything that is not a real WebSocket handshake request.
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    // Complete the handshake and get the live socket.
    using WebSocket socket = await context.WebSockets.AcceptWebSocketAsync();
    Console.WriteLine("[SERVER] Client connected.");

    // One token source that lets us stop BOTH loops together when either the
    // client leaves or the receive loop decides the conversation is over.
    using var cts = new CancellationTokenSource();

    // Start both directions concurrently on the SAME connection.
    Task receiveLoop = ReceiveFromClientAsync(socket, cts);
    Task notifyLoop = NotifyClientAsync(socket, cts.Token);

    // Wait until both loops finish. Task.WhenAll lets them run at the same time
    // rather than one-after-the-other.
    await Task.WhenAll(receiveLoop, notifyLoop);

    Console.WriteLine("[SERVER] Client disconnected.");
});

app.Run();

// ---------------------------------------------------------------------------
//  RECEIVE LOOP : reads messages the client sends, and replies to each one.
// ---------------------------------------------------------------------------
static async Task ReceiveFromClientAsync(WebSocket socket, CancellationTokenSource cts)
{
    var buffer = new byte[4 * 1024];

    try
    {
        while (socket.State == WebSocketState.Open && !cts.IsCancellationRequested)
        {
            // Wait for the next chunk of data from the client. This does NOT
            // burn a thread while waiting — the OS wakes us when data arrives.
            WebSocketReceiveResult result =
                await socket.ReceiveAsync(buffer, cts.Token);

            // The client asked to close the connection politely.
            if (result.MessageType == WebSocketMessageType.Close)
            {
                await socket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Closing as requested",
                    CancellationToken.None);
                cts.Cancel(); // stop the notify loop too
                break;
            }

            // Turn the received bytes back into a string (UTF-8).
            string text = Encoding.UTF8.GetString(buffer, 0, result.Count);
            Console.WriteLine($"[SERVER] Received: {text}");

            // Reply directly to this message.
            await SendTextAsync(socket, $"Hello, client! You said: \"{text}\"", cts.Token);
        }
    }
    catch (OperationCanceledException)
    {
        // Normal during shutdown — ignore.
    }
    catch (WebSocketException ex)
    {
        Console.WriteLine($"[SERVER] Receive error: {ex.Message}");
        cts.Cancel();
    }
}

// ---------------------------------------------------------------------------
//  NOTIFY LOOP : pushes messages to the client on a timer, UNPROMPTED.
//  This is the part a plain HTTP server cannot do.
// ---------------------------------------------------------------------------
static async Task NotifyClientAsync(WebSocket socket, CancellationToken token)
{
    int counter = 1;

    try
    {
        while (socket.State == WebSocketState.Open && !token.IsCancellationRequested)
        {
            // Wait 3 seconds between notifications without blocking a thread.
            await Task.Delay(TimeSpan.FromSeconds(3), token);

            if (socket.State == WebSocketState.Open)
            {
                await SendTextAsync(socket, $"Server notification #{counter++}", token);
            }
        }
    }
    catch (OperationCanceledException)
    {
        // Normal during shutdown — ignore.
    }
    catch (WebSocketException ex)
    {
        Console.WriteLine($"[SERVER] Notify error: {ex.Message}");
    }
}

// ---------------------------------------------------------------------------
//  Helper: send one text message. A WebSocket "message" can span multiple
//  frames, but here each message fits in one frame, so endOfMessage: true.
// ---------------------------------------------------------------------------
static async Task SendTextAsync(WebSocket socket, string message, CancellationToken token)
{
    byte[] bytes = Encoding.UTF8.GetBytes(message);
    await socket.SendAsync(
        new ArraySegment<byte>(bytes),
        WebSocketMessageType.Text,
        endOfMessage: true,
        token);
    Console.WriteLine($"[SERVER] Sent: {message}");
}
