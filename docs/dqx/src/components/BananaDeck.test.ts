import assert from 'node:assert/strict';
import test from 'node:test';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';

import BananaDeck, { BananaSlide, ExplanationExample } from './BananaDeck';

const renderSlide = (index: number): string =>
  renderToStaticMarkup(createElement(BananaSlide, { index }));

test('the deck renders without browser globals and exposes all 13 slides', () => {
  const html = renderToStaticMarkup(createElement(BananaDeck));
  assert.equal((html.match(/<option /g) ?? []).length, 13);
  assert.match(html, /The odd banana/);
  assert.match(html, /Learning from historical data/);
  assert.match(html, /aria-label="Choose slide"/);
  assert.match(html, /aria-label="Previous slide" disabled/);
  assert.match(html, /aria-label="Next slide"/);
  assert.match(html, /aria-valuemax="13" aria-valuenow="1"/);
});

for (let index = 0; index < 13; index += 1) {
  test(`slide ${index + 1} renders on the server with no em dashes`, () => {
    const html = renderSlide(index);
    assert.ok(html.length > 0);
    assert.doesNotMatch(html, /\u2014/);
  });
}

test('the cover distinguishes a rule pass from an anomaly review', () => {
  const html = renderSlide(0);
  assert.match(html, /RULES/);
  assert.match(html, /✓ OK/);
  assert.match(html, /#42 · REVIEW/);
  assert.match(html, /aria-label="Pause animation"/);
  assert.match(html, /aria-label="Copy installation command"/);
  assert.doesNotMatch(html, /TERMINAL/);
});

test('DQM and DQX remain complementary layers in the comparison', () => {
  const html = renderSlide(1);
  assert.match(html, /Databricks DQM/);
  assert.match(html, /Is the table fresh/);
  assert.match(html, /Does this row meet our rules/);
  assert.match(html, /Use both layers/);
});

test('the profiles and baseline notes do not promise sequence forecasting or automatic retraining', () => {
  const html = renderToStaticMarkup(createElement(BananaDeck));
  assert.match(renderSlide(4), /profile=&quot;tabular&quot;/);
  assert.match(renderSlide(4), /profile=&quot;correlation&quot;/);
  assert.match(
    renderSlide(11),
    /Neither baseline option retrains automatically/,
  );
  assert.doesNotMatch(html, /profile=&quot;timeseries&quot;/);
});

test('the relationship example puts both measurements inside the historical ranges', () => {
  const html = renderSlide(5);
  assert.match(html, /22/);
  assert.match(html, /94/);
  assert.match(html, /Both values fit the historical ranges/);
  assert.match(html, /a clue, not a diagnosis/);
});

test('the example threshold starts with four flags and avoids probability claims', () => {
  const html = renderSlide(8);
  assert.match(html, /<strong>4<\/strong> of 8 example rows flagged/);
  assert.match(html, /95 does not mean a 95% chance of error/);
});

test('the complete explanation uses group-level numerical evidence and conditional impact', () => {
  const html = renderToStaticMarkup(
    createElement(ExplanationExample, {
      redacted: false,
      onToggle: () => {},
    }),
  );
  assert.match(html, /12 example flagged rows/);
  assert.match(html, /99.2 \/ 100/);
  assert.match(html, /not a probability of error/);
  assert.match(html, /weight \(65%\) and length \(30%\)/);
  assert.match(html, /If weight is wrong/);
  assert.match(html, /Suggested action/);
  assert.match(html, /aria-pressed="false"/);
});

test('redaction recalculates disclosed shares without hiding the original contributions', () => {
  const html = renderToStaticMarkup(
    createElement(ExplanationExample, {
      redacted: true,
      onToggle: () => {},
    }),
  );
  const output = html.slice(html.indexOf('<aside>'), html.indexOf('</aside>'));
  assert.match(output, /length \(86%\) and ripeness \(14%\)/);
  assert.match(
    output,
    /Most of the contributing evidence could not be disclosed/,
  );
  assert.doesNotMatch(output, /weight/i);
  assert.match(output, /aria-pressed="true"/);
  assert.match(html, /<span>Weight<\/span><strong>65%<\/strong>/);
  assert.match(html, /<span>Length<\/span><strong>30%<\/strong>/);
  assert.match(html, /<span>Ripeness<\/span><strong>5%<\/strong>/);
  assert.match(html, /99.2 \/ 100/);
});

test('the closing slide offers installation and the opening banana joke', () => {
  const html = renderSlide(12);
  assert.match(html, /databricks-labs-dqx\[anomaly\]/);
  assert.match(html, /aria-label="Copy installation command"/);
  assert.match(html, /No bananas were harmed. A few were flagged for review./);
});
