import 'dart:async';

/// Lets the client abort work it handed to a connector or an authorizer, the
/// way Dio's `CancelToken` aborts a request: closing a pending `HttpClient`,
/// dropping a socket that is still upgrading.
///
/// A connector or authorizer listens with [whenCancelled] or [isCancelled];
/// only the client cancels.
final class ReverbCancelToken {
  /// A live token.
  ReverbCancelToken();

  final Completer<Object> _cancelled = Completer<Object>();

  /// Whether [cancel] has run.
  bool get isCancelled => _cancelled.isCompleted;

  /// Completes with the cancellation reason when [cancel] runs.
  Future<Object> get whenCancelled => _cancelled.future;

  /// The reason [cancel] was given, or null while live.
  Object? get reason => _reason;
  Object? _reason;

  /// Cancels with [cause]. Later calls do nothing.
  void cancel([Object cause = 'cancelled']) {
    if (_cancelled.isCompleted) return;
    _reason = cause;
    _cancelled.complete(cause);
  }
}
