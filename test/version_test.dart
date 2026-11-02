@TestOn('vm')
library;

import 'dart:io';

import 'package:dart_reverb_channels/dart_reverb_channels.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

// Reads pubspec.yaml from disk, so it runs on the VM only.
void main() {
  test('the version constant matches pubspec.yaml', () {
    final pubspec = File('pubspec.yaml').readAsStringSync();
    final version = RegExp(r'^version:\s*(\S+)', multiLine: true).firstMatch(pubspec)!.group(1);

    expect(dartReverbChannelsVersion, version);
    expect(testOptions.socketUri.queryParameters['version'], version);
  });
}
