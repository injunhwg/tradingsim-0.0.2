const test = require('node:test');
const assert = require('node:assert/strict');

const { buildPublicInfoSchedule, DEFAULT_GAME_DURATION_SECONDS } = require('../src/services/classroomGame');

test('buildPublicInfoSchedule uses the 15-minute default and spaces EPS evenly', () => {
  assert.equal(DEFAULT_GAME_DURATION_SECONDS, 900);
  assert.deepEqual(
    buildPublicInfoSchedule().map((item) => [item.infoType, item.sequenceNo, item.scheduledOffsetSeconds]),
    [
      ['SAR', 1, 0],
      ['EPS', 1, 180],
      ['EPS', 2, 360],
      ['SAR', 2, 450],
      ['EPS', 3, 540],
      ['EPS', 4, 720]
    ]
  );
});
