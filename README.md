# jcollab

MCP server (stdio) that lets Claude read, edit and run cells in live JupyterLab notebooks through the shared Yjs
document, so open browser tabs update immediately and cells look exactly as if the user had run them.

* **Discovery, no setup:** finds all running Jupyter servers of the user via `~/.local/share/jupyter/runtime/jpserver-*.json`
  (pid alive + `/api/status` probe). Tools take `instance` (port, `root:port` or root dir name); optional if only one runs.
* **Verified writes:** every mutating tool reads the file back and reports `persisted`; duplicate cell ids (client/server
  Yjs version skew) are detected and explained.
* **Execution fidelity:** `[*]` busy state, streamed outputs, kernel execution count, `metadata.execution` timings
  (jupyterlab-execute-time). `run_code` is silent (`store_history=False`), consumes no execution count.
* **Tools:** list_instances, list_notebooks, read_notebook, read_cell (images), insert_cell, insert_cell_at_start, edit_cell,
  change_cell_type, delete_cell, move_cell, split_cell, merge_cells, get_metadata, set_metadata (cell/notebook),
  run_cell, run_cells, run_code, clear_outputs, inspect_variables, kernel_status, interrupt_kernel, restart_kernel,
  sync_health.

## Install

    python3 -m venv .venv && .venv/bin/pip install .
    claude mcp add --scope user jcollab -- $PWD/.venv/bin/jcollab

Requires a JupyterLab with `jupyter-collaboration`. Keep `jupyter-ydoc` of this package on the same major version as the
server's (pinned `>=3,<4`; see `sync_health`).

## Tests

`tests/integration.py [instance]` expects a throwaway JupyterLab (token-less) whose root contains `spike.ipynb`.
