part of 'client.dart';

/// One owner's hold on a channel, returned by [ReverbClient.subscribe].
///
/// Several subscriptions to one channel share one Pusher subscription; the
/// channel is left when the last of them is cancelled.
final class ReverbSubscription {
  ReverbSubscription._(this.channel, this._client) {
    // The outcome is optional reading: a subscription nobody asks about must
    // not report its failure as uncaught. Readers still receive it.
    _ready.future.ignore();
  }

  /// A subscription that failed before it was made: [ready] fails with
  /// [error], [events] stays open and silent until [cancel].
  ReverbSubscription._failed(this.channel, ReverbException error) : _client = null {
    _ready.future.ignore();
    _fail(error);
  }

  /// The channel, with its prefix.
  final String channel;

  final ReverbClient? _client;
  _ChannelEntry? _entry;

  final Completer<void> _ready = Completer<void>();
  final StreamController<ReverbEvent> _controller = StreamController<ReverbEvent>.broadcast();
  late final Stream<ReverbEvent> _events = _controller.stream.where((_) => !_cancelled);
  bool _cancelled = false;
  bool _succeeded = false;

  /// Completes once the channel is subscribed, or with the [ReverbException]
  /// that ended the attempts: the retry policy ran out, a failure was fatal,
  /// or [cancel] / [ReverbClient.closeAll] came first
  /// ([ReverbCancelledException]). It completes once; a later drop and
  /// resubscribe do not change it.
  Future<void> get ready => _ready.future;

  /// Whether [ready] completed successfully.
  bool get isReady => _succeeded;

  /// Whether [cancel] or [ReverbClient.closeAll] ended this subscription.
  bool get isCancelled => _cancelled;

  /// The channel's events, as a broadcast stream. It is done only after
  /// [cancel] or [ReverbClient.closeAll], and delivers nothing after either.
  /// A failed subscription keeps it open and silent.
  Stream<ReverbEvent> get events => _events;

  /// The events named [event].
  Stream<ReverbEvent> on(String event) => _events.where((ReverbEvent e) => e.name == event);

  /// The members of a presence channel, keyed by id. Empty for other kinds
  /// and while not subscribed.
  Map<String, PresenceMember> get members {
    final entry = _entry;
    if (entry == null || _cancelled) return const <String, PresenceMember>{};
    return Map<String, PresenceMember>.unmodifiable(entry.members);
  }

  /// This client's own member on a presence channel, once subscribed.
  PresenceMember? get me {
    final entry = _entry;
    final id = entry?.myId;
    if (entry == null || id == null || _cancelled) return null;
    return entry.members[id] ?? PresenceMember(id: id);
  }

  /// Sends a client event (a whisper) on a private or presence channel.
  ///
  /// The `client-` prefix is added when missing. Returns false when the
  /// channel is not subscribed right now. Throws [StateError] after [cancel]
  /// and on channels that do not accept client events.
  bool trigger(String event, [Object? data]) {
    if (_cancelled) throw StateError('Subscription to $channel is cancelled');
    final client = _client;
    if (client == null) return false;
    return client._trigger(this, event, data);
  }

  /// Ends this subscription: fails a pending [ready] with
  /// [ReverbCancelledException], ends [events] and releases the channel.
  /// Calling it again does nothing.
  Future<void> cancel() async {
    if (_cancelled) return;
    _terminate(const ReverbCancelledException('Subscription cancelled'));
    _client?._detach(this);
  }

  void _succeed() {
    if (_ready.isCompleted || _cancelled) return;
    _succeeded = true;
    _ready.complete();
  }

  void _fail(ReverbException error, [StackTrace? stackTrace]) {
    if (_ready.isCompleted) return;
    _ready.completeError(error, stackTrace);
  }

  void _deliver(ReverbEvent event) {
    if (_cancelled || _controller.isClosed) return;
    _controller.add(event);
  }

  void _terminate(ReverbException reason) {
    if (_cancelled) return;
    _cancelled = true;
    _fail(reason);
    unawaited(_controller.close());
  }
}
