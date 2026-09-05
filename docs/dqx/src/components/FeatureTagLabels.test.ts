import assert from 'node:assert/strict';
import test from 'node:test';

import { availableSinceLabel } from './FeatureTagLabels';

test('availableSinceLabel includes the product name', () => {
  assert.equal(
    availableSinceLabel('DQX Studio', '0.1.0'),
    'Available since DQX Studio v0.1.0',
  );
});
