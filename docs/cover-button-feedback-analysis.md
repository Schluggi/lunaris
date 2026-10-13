# Cover-button feedback: native Sensor command

The previous `SetAlarm`-based feedback was not equivalent to the stock tap confirmation. Hardware testing found that a one-second alarm was not perceptible, while longer alarms were, even at full intensity. An approximately one-second motor-start delay was a hypothesis, not a measured fact. The Sensor firmware explains why raising the requested alarm intensity alone was insufficient: alarm patterns use a power ramp, unlike native tap patterns.

Static inspection of `../rea/frankenfirmware` (SHA256 `4b8a16ae71ba3549c788d9260e0a37f855ed7c5a867b8bdab07de508dd825614`) found another response path:

- `handleGesture` at `0x57ed0` sends command `0x40` through the Sensor object's virtual sender with four data bytes `[side, 0x1e, code, count]`.
- At `0x58088`–`0x580bc`, a successful two-tap adjustment constructs `[side, 0x1e, 2, 2]`; the corresponding three-tap path constructs `[side, 0x1e, 3, 3]`.
- Failed adjustments construct `[side, 0x1e, 5, 1]`; the four-or-more-tap branch constructs `[side, 0x1e, 4, 4]`.
- `handleAlarm` at `0x581f0` also sends command `0x40`, including `[side, 0x1e, 4, 2]`.

The previous conclusion that there was no tap-feedback command was based only on callers of `triggerVibrationAlarm` (`0x2C`) and missed this separate path. The earlier `0x1A` inference was also incorrect: it is an internal coroutine resume-state value, not an established wire opcode.

A 500 ms host-side cancel was not enabled. The implementation now uses the native command below instead.

## Sensor MCU evidence

Target: `../rea/firmware-sensor-g0.bbin`, 80,996 bytes, SHA256 `01cb4bf393dc04d2e1c6c693bd13097ba70fe9fe6e7b6b3d4e52381f07924c9b`. This is the supplied variant containing the `[lis%s] dismissing alarm (%u taps)` messages observed on the user's Pod. The other two supplied variants were inventoried, not fully analyzed. No firmware was executed or flashed.

Ghidra 12.1.3 static analysis: raw ARM little-endian Cortex/Thumb image loaded at `0x08008000`, including its 128-byte header; address = file offset + `0x08008000`. The vector table starts at file offset `0x80`. Reset at `0x08013844` copies flash `0x0801b85c` to RAM `0x20000200`–`0x20000608`; this relocation is required to read the pattern sequence pointers correctly. Analysis project, scripts, and reports are under `/tmp/opencode/sensor-analysis` and `/tmp/opencode/sensor-*.txt`.

| Location | Static observation |
| --- | --- |
| `0x0801343e`–`0x0801344c` | Command dispatch subtracts `0x20`; table case `0x20` therefore corresponds to command `0x40`. |
| `0x080137ca`–`0x0801380a` | Reads `[side, power, pattern, pcnt]` after the command byte. `pcnt == 0` stops the side; otherwise duration = `pcnt * 250` ms. Log format: `[cmd] side=%u pow=%u patt=%u pcnt=%u`. |
| `0x0800cd88` / `0x0800ccd4` | Starts the selected side with that millisecond duration; side 2 starts both sides. An already-running side is not restarted by the start helper. |
| `0x08013694`–`0x0801369e` / `0x0800cd40` | Ordinary `0x2C` reads BE32 seconds and multiplies by 1000. |
| Pattern pointer table `0x08019ae0` | Patterns 0/1 use power callback `0x0800d0e4`, a time-dependent ramp with a minimum output of 5. Patterns 2–5 use `0x08008ed6`, which returns the requested power directly. |
| Patterns 2/3/4: descriptor `0x08019c5c`, steps `0x08019b38` | Repeating 250 ms cycle: 125 ms motor-on and 125 ms off. |
| Pattern 5: descriptor `0x08019c50`, step `0x08019b30` | One continuously motor-on step lasting 250 ms, repeated until the requested duration expires; no ramp. |
| `0x08010754` / `0x080107b8` | Advances pattern steps, writes motor outputs, and stops at maximum duration. Pattern 5's one-step sequence remains on across its cycles. |
| `0x08010adc` | Tap processing waits for more than 499 ms on its tap timer before reporting the accumulated count. A multi-tap gesture also emits `0x36` with side/count. |

The hub's `0x1e` byte is power 30, not a timing constant. Its codes 2/3/4 select short alternating tap patterns; code 5 selects continuous feedback. The final byte sets the total duration in quarter-second units, rather than directly encoding a count independently of the selected pattern.

## Stock waveform implementation and remaining verification

The first native implementation used configurable continuous 500 ms pulses at pattern 5, separated by host gaps. The user confirmed that this worked, then requested the stock waveform and only a general on/off switch.

Lunaris now sends one `0x40` command per reported gesture with fixed power 30 and the stock alternating tap patterns: `[side, 30, 2, 2]` for two taps, `[side, 30, 3, 3]` for three, `[side, 30, 4, 4]` for four or more. Each cycle has 125 ms vibration and 125 ms pause; the MCU handles all timing. The existing single-tap confirmation uses `[side, 30, 2, 1]`, one cycle of the same waveform. This single-tap extension is not a recovered hub call: the inspected hub gesture handler ignores counts below two. Context-dependent hub heat-adjustment/error responses are not emulated by this general confirmation feature.

No host-side pulse gap or early cancel is used. Ordinary vibration buttons still use `0x2C` with their separate settings. The feedback switch defaults on, restores retained on/off state, and suppresses the whole command when off. Intensity/duration controls, subscriptions, and retained-setting ingestion have been removed. Empty retained discovery messages remove those old HA entities on MQTT connect, even if the Sensor is disabled.

The native command path is hardware-confirmed; the shorter fixed stock waveform needs a new hardware check. Tap-report aggregation and an unarmed link's priming fallback can still delay feedback. This does not claim instantaneous response or complete stock gesture/heat-adjustment behavior. Earlier per-press events would require separate wire evidence and deduplication before changing the trigger.
