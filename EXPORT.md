# Source export

Prepared from the running InventionLab code on 11 November 2026.

Current `knight.py` SHA-256: `b8a944a0775b7af0e28d83ee1854d661f95a1dd6e0936dbedb2618eaa6612d0b`

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
