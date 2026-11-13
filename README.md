# Knight

InventionLab’s Python terminal interface for persistent AI agents on Linux.

Agents execute Bash, work with ordinary files and processes, and keep a saved
conversation. The operator chooses which parts of that conversation enter the
next model request. Commands, results, images and usage remain inspectable.

## What you can do

- Resume a named session across restarts.
- Send a chat reply request, or allocate a batch of tool-using pulses.
- Switch conversation groups in and out of the active context.
- Return a screenshot path from Bash to give the model the image.
- Exchange messages between agents through the included `tools/wire.py`.
- Open optional graphical context controls with PyQt6.

## Run

Requires Linux, Python 3.10+ with `venv`/`pip`, Bash, Git and access to the
OpenAI Responses API. Run these commands from this checkout in Bash.

```bash
git clone https://github.com/erenyilmazf/knight.git
cd knight
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

export KNIGHT_HOME="${KNIGHT_HOME:-$HOME/.local/share/knight}"
mkdir -p "$KNIGHT_HOME"
git -C "$KNIGHT_HOME" init
git -C "$KNIGHT_HOME" config user.name "Knight"
git -C "$KNIGHT_HOME" config user.email "knight@localhost"

read -rsp 'OpenAI API key: ' OPENAI_API_KEY; echo
export OPENAI_API_KEY
python3 -B knight.py --session first
```

Type a message and press Enter to save it. F3 requests a text response.
Tab selects the Calls field; F4 runs that many tool-using responses.
F2 records voice; F5 shows the last reply; F6 opens context; F7 adds files.
Ctrl-D closes the console. Reopen with the same `--session` name to continue.

Input tokens are counted automatically through the API after saved changes settle.
Counting runs in the background; unfinished typing is excluded. The display says
`(saved text)` for the next text-request preview, or `(request)` for the count
used before generation. No model reply is requested by the background counter.
Use `python3 -B knight.py --list` to list saved session names.

F8 switches between message entry and actual Python. In Python, `app` and
`session` are the running console objects. For example:

```python
app.settings("model", "YOUR_MODEL_ID")
session.config
app.say("A message to save")
```

Choose a model, effort and request limits compatible with your account before
requesting a response. Press F8 again to return to ordinary message entry.
To change Bash's starting directory from Python:

```python
session.state["cwd"] = str(Path.home())
session.save()
```

Shell commands run with the permissions of the Unix user launching Knight.
Model requests use that user’s API account.

Session files live in `~/.local/share/knight/sessions/`. Set `KNIGHT_HOME` to
choose another data directory, outside the source checkout. The shared Wire
journal is `$KNIGHT_HOME/messages.jsonl`. The local Git setup above is required
for context switching and deletion: each change preserves a
checkpoint. Session JSON is saved even when Git recording fails.

No Git remote is needed. Automatic pushes default to off;
`app.record_git("manual", push=True)` pushes if an upstream is configured. Keep the data repository
separate from any public source repository. Deleting context does not erase its
older Git checkpoints.

For the graphical context view, install `PyQt6` in the same Python environment.
**Add text…** opens a multiline box for pasting or typing a message.
**Add** saves it into context; F3 or F4 requests the next response.
Check entries, then use **Turn off checked** or **Turn on checked** to change
their inclusion. **Mark \*** and **Unmark** add or remove a bookmark, without
changing inclusion. Choose **Marked \*** to find those entries. Old named
bookmarks become independent marks on reopening, preserving their ON/OFF state.
Without PyQt6 or a display, F6 shows a text table;
`app.show_context(json_output=True)` returns its rows as JSON.
For F2 microphone transcription, install `openai` in the same environment
and PipeWire’s `pw-record` command. The helper uses the transcription model
configured in `tools/voice.py`.

## Wire

Use the session ID printed when the console opens as the recipient, or set
`LAB_WIRE_HANDLE` before launch to give a session an additional address.

```bash
python3 tools/wire.py say YOUR_ID "Message text" --to RECIPIENT_ID
python3 tools/wire.py read --for YOUR_ID
```

Consoles import addressed messages before a request; writing a Wire message
does not wake a console or allocate calls. Messages sent by a session to
`human` are also retained in that session. `LAB_WIRE_FILE` overrides the journal
path for both tools; `LAB_WIRE_RECEIVE=0` disables automatic intake.

## A first task

In a working directory of your choice, ask an agent to inspect a small source
file, run it, and explain the result. Keep the conversation open, toggle an old
entry in the F6 context window, and inspect the next request. The saved `session.json`
records the interaction for later review.

## Project

Built by Eren Yilmaz and AI agents as part of InventionLab. Knight is used to
operate long-running agents, develop Linux tools and investigate real systems.

One robotics application uses competitive grSim soccer controllers. The
underlying Western Sydney University RoboCup project is described in
[TurtleRabbit 2024 SSL Team Description Paper](https://arxiv.org/abs/2402.08205),
[DOI 10.48550/arXiv.2402.08205](https://doi.org/10.48550/arXiv.2402.08205).

This first source export contains Knight and its local Wire/voice helpers.
RoboCup demonstrations and the shared Linux environment are prospective
companion releases.

## Licence

Copyright © 2026 Eren Yilmaz.

Licensed under the [GNU General Public License v3.0](LICENSE)
(`GPL-3.0-only`). You may use, study, modify and redistribute Knight under
that licence. Distributed without warranty.
