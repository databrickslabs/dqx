import assert from 'node:assert/strict';
import test from 'node:test';

import {
  availableSinceLabel,
  deprecatedInLabel,
} from './FeatureTagLabels';

test('availableSinceLabel names the Core product', () => {
  assert.equal(
    availableSinceLabel('DQX', '0.16.0'),
    'Available since DQX v0.16.0',
  );
});

test('availableSinceLabel distinguishes Studio releases', () => {
  assert.equal(
    availableSinceLabel('DQX Studio', '0.1.0'),
    'Available since DQX Studio v0.1.0',
  );
});

test('deprecatedInLabel names the independently versioned product', () => {
  assert.equal(
    deprecatedInLabel('DQX Studio', '0.1.0'),
    'Deprecated in DQX Studio v0.1.0',
  );
});
