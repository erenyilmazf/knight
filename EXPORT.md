# Source export

Prepared from the running InventionLab code on 11 November 2026.

Current `knight.py` SHA-256: `56c878a54d63b6527d64d97ddccb253382a42ce9e72c9f85f0804bb4359f423a`

Export adjustments: checkout-relative Wire/voice helper paths, a shared
`KNIGHT_HOME` data directory, automatic Git pushes off by default, and the
operator Wire address `human`. The lab’s running copies are unchanged.

Disposable first-run checks passed dependency installation, terminal input,
session reopen, local Git checkpoints, context controls and Wire routing.
The graphical context was checked offscreen; headless text fallback also passed.
No provider requests or microphone recordings were made.

GPL-3.0 publication approved by Eren on 11 November 2026. Licence notices
were added after the first-run review.

Public source: https://github.com/erenyilmazf/knight

11 November: removed the unused group byte-count helper and no-op API event
callback. Request bytes, response parsing and export adjustments are unchanged.

13 November: show Bash-opportunity counters and request effort; label input
preparation and response receipt times. Old HUDs and captures remain readable.
Execution, memory controls and export adjustments are unchanged.

13 November: ordinary Bash cd carries into the next command and session save.
The HUD defines an action and omits the lifetime API number. Exact captures
match saved replies through local request hashes; old API-number captures work.
Directory carry-over uses an EXIT trap; exec or replacing the trap bypasses it.

Directory capture preserves CR/LF bytes and suppresses its own xtrace output.
An undecodable directory name leaves cwd unchanged and prints a warning; the
command result is retained.

13 November purge: named collections replaced by independent star marks; old
record conversion removed from ordinary reads/saves. Removed the extra composer,
slash-command parser and captured-request comparison window. F6 opens context,
F7 loads files, and F8 switches between text and actual Python (app/session).
Existing F2/F3/F4/F5 controls, time cues and selection-size measurements remain.
