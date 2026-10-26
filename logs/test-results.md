Chit Chat Test Results

Test: Three clients, join notifications, message broadcasts, and graceful departures

Test 1: Client joins
Result: Passed for the tested scenario.

Three independent client processes, X, Y, and Z, connected to the server. The server assigned logical timestamps 1, 2, and 3 to their respective join notifications. The log shows each notification being enqueued for all participants active after the corresponding join.


Test 2: Message broadcast
Result: Passed for the tested scenario.

With all three clients connected, the clients published the following messages:
X: Hello everyone! — logical timestamp 4
Y: Hi X! — logical timestamp 5
Z: Hello X and Y! — logical timestamp 6

The server log shows each message being enqueued for X, Y, and Z.


Test 3: Client leaves
Result: Passed for the tested scenario.

Client X left at logical timestamp 7. The server enqueued the leave notification for Y and Z.
Client Y then left at logical timestamp 8. The server enqueued the leave notification for Z.
Finally, Z left at logical timestamp 9. No other participants remained to receive this final notification.


Test 4: Message validation
Result: Client-side validation tested.

The client implementation rejects messages exceeding 128 Unicode characters and checks UTF-8 validity before publishing. An oversized message was rejected by the client in a separate test.


Test 5: Concurrent clients and message delivery
Result: Passed for the tested three-client scenario.

Three independent client processes connected to one server. Each client published a message while all three clients were active. The server log records the broadcasts and enqueuing for all three recipients.


Test 6: Graceful client departure
Result: Passed for the tested sequence.

The server processed the three leave requests in sequence and assigned logical timestamps 7, 8, and 9. Each departure notification was enqueued for the participants remaining at that point.


Conclusion
The integration test demonstrates the main Chit Chat functionality: participant joins, message publication, broadcast to active participants, monotonically increasing server logical timestamps, and participant departures. The server log records broadcast and delivery-to-stream events with sender and recipient identifiers.