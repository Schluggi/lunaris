/** @see https://commitlint.js.org/#/reference-rules */
export default {
  extends: ['@commitlint/config-conventional'],
  rules: {
    // Allow MQTT, LED, ON/OFF, etc. in subjects without lowercase-only enforcement.
    'subject-case': [0],
  },
};
