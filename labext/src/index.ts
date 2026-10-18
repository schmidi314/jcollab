import { JupyterFrontEnd, JupyterFrontEndPlugin } from '@jupyterlab/application';
import { Notification } from '@jupyterlab/apputils';
import { INotebookTracker, NotebookPanel } from '@jupyterlab/notebook';
import type { IAwareness } from '@jupyter/ydoc';

interface IScrollTo { cell: string; nonce: string; force?: boolean; label?: string | null }
type Change = { added: number[]; updated: number[]; removed: number[] };

function request(state: any): IScrollTo | null {
  const s = state && state.scrollTo;
  return s && typeof s.cell === 'string' && typeof s.nonce === 'string' ? s : null;
}
function who(state: any): string {
  const u = (state && state.user) || {};
  return u.display_name || u.name || 'A collaborator';
}
function indexOf(panel: NotebookPanel, cellId: string): number {
  return panel.content.widgets.findIndex(c => c.model.id === cellId);
}
function typingElsewhere(panel: NotebookPanel, index: number): boolean {
  const nb = panel.content;
  const a = nb.activeCell;
  return nb.mode === 'edit' && !!a && !!a.editor && a.editor.hasFocus() && nb.activeCellIndex !== index;
}
function scrollToId(panel: NotebookPanel, cellId: string): void {
  const i = indexOf(panel, cellId);
  if (i >= 0) {
    panel.content.scrollToItem(i, 'smart').catch(e => console.warn('jcollab-scroll:', e));
  }
}

function watch(panel: NotebookPanel): void {
  const aw = panel.context.model.sharedModel.awareness as IAwareness | undefined;
  if (!aw) { console.warn('jcollab-scroll: no awareness (jupyter-collaboration off?)'); return; }
  const seen = new Map<number, string>();
  aw.getStates().forEach((st, id) => { const r = request(st); if (r) { seen.set(id, r.nonce); } });
  const onChange = (ch: Change) => {
    for (const id of [...ch.added, ...ch.updated]) {
      if (id === aw.clientID) { continue; }
      const st = aw.getStates().get(id);
      const r = request(st);
      if (!r || seen.get(id) === r.nonce) { continue; }
      seen.set(id, r.nonce);
      const i = indexOf(panel, r.cell);
      if (i < 0) { continue; }
      if (!r.force && typingElsewhere(panel, i)) {
        Notification.info(`${who(st)} points at cell ${i + 1}${r.label ? ': ' + r.label : ''}`, {
          autoClose: 10000, actions: [{ label: 'Show', callback: () => scrollToId(panel, r.cell) }] });
        continue;
      }
      scrollToId(panel, r.cell);
    }
  };
  aw.on('change', onChange);
  panel.disposed.connect(() => aw.off('change', onChange));
}

const plugin: JupyterFrontEndPlugin<void> = {
  id: 'jcollab-scroll:plugin', autoStart: true, requires: [INotebookTracker],
  activate: (_app: JupyterFrontEnd, tracker: INotebookTracker) => {
    const done = new WeakSet<NotebookPanel>();
    const add = (p: NotebookPanel) => {
      if (done.has(p)) { return; }
      done.add(p);
      p.context.ready.then(() => watch(p)).catch(() => undefined);
    };
    tracker.forEach(add);
    tracker.widgetAdded.connect((_t, p) => add(p));
    console.log('jcollab-scroll activated');
  }
};
export default plugin;
