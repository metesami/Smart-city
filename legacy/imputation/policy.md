| missing_reason | رفتار                    |
| -------------- | ------------------------ |
| NONE           | استفاده مستقیم           |
| ZERO_RUN_SHORT | impute (temporal)        |
| SPIKE          | impute (local smoothing) |
| PROFILE_SOFT   | impute (profile-based)   |
| PROFILE_HARD   | mask (no impute)         |
| STUCK_OFF / ON | mask                     |
| PHYS_INVALID   | mask                     |
| CAP_EXCEEDED   | mask                     |
| LOGIC_INVALID  | mask                     |





| Layer | missing_reason | روش            | هدف               |
| ----- | -------------- | -------------- | ----------------- |
| L1    | ZERO_RUN_SHORT | linear interp  | continuity        |
| L2    | SPIKE          | rolling median | denoise           |
| L3    | PROFILE_SOFT   | time profile   | seasonal recovery |
