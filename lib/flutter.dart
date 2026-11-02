/// Flutter glue for `dart_reverb_channels`.
library;

import 'package:flutter/widgets.dart';

import 'dart_reverb_channels.dart';

export 'dart_reverb_channels.dart';

/// Pauses a [ReverbClient] while the app is in the background and resumes it
/// in the foreground, so iOS does not leave a socket the OS has silently
/// killed in place. Opt-in: attach it where the app composes its services and
/// [detach] it on teardown.
final class ReverbLifecycleBinding with WidgetsBindingObserver {
  /// Observes the app lifecycle for [client].
  ReverbLifecycleBinding(this.client);

  /// The client to pause and resume.
  final ReverbClient client;

  bool _attached = false;

  /// Starts observing.
  void attach() {
    if (_attached) return;
    _attached = true;
    WidgetsBinding.instance.addObserver(this);
  }

  /// Stops observing.
  void detach() {
    if (!_attached) return;
    _attached = false;
    WidgetsBinding.instance.removeObserver(this);
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    switch (state) {
      case AppLifecycleState.paused:
      case AppLifecycleState.detached:
        client.pause();
      case AppLifecycleState.resumed:
        client.resume();
      case AppLifecycleState.inactive:
      case AppLifecycleState.hidden:
        break;
    }
  }
}
