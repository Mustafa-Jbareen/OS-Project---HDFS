"""Figures of the final report (final-report.py). Needs matplotlib.

Five figures, each tied to one part of the conclusion:
  1 runtime vs k          -- how big is the cost, from which k, load or cache?
  2 where the time goes   -- map task time, DataNode time per block, CPU per task
  3 storage stack alone   -- the same disks read without Hadoop
  4 controls              -- does the slowdown survive other ways of building the disks?
  5 server cost vs k      -- DataNode threads / memory, NameNode heap, block reports, ...

Style (static, light surface): concurrency levels are ordinal, so they take one
blue ramp, light = little concurrency, dark = a lot, and a level keeps its
color in every panel and figure; cold vs warm cache are separate panels, never
a second color. Thin lines, 95% CI as a light band, solid hairline grid, a
solid zero line, ticks on round values only, direct labels at line ends only
where they do not collide. Every figure is saved as PNG (report) and PDF.
"""
import math

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import FuncFormatter, MaxNLocator, NullLocator  # noqa: E402

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
BLUE = "#2a78d6"
# Ordinal blue ramp, validated light -> dark (steps 250, 450, 650).
RAMP = ["#86b6ef", "#2a78d6", "#104281"]

# Space at the top of every figure for title, subtitle and legend (inches).
HEADER_WITH_LEGEND_IN = 0.95
HEADER_IN = 0.72

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 9,
    "axes.titlesize": 9.5,
    "axes.titlecolor": INK,
    "axes.labelcolor": INK2,
    "axes.labelsize": 8.5,
    "axes.edgecolor": AXIS,
    "axes.linewidth": 0.8,
    "axes.facecolor": SURFACE,
    "figure.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "xtick.labelcolor": INK2,
    "ytick.labelcolor": INK2,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.frameon": False,
    "legend.labelcolor": INK2,
})


def level_colors(levels):
    """Fixed color per concurrency level, ranked over all levels in the figure."""
    levels = sorted(set(levels))
    if len(levels) == 1:
        return {levels[0]: RAMP[1]}
    if len(levels) == 2:
        return {levels[0]: RAMP[0], levels[1]: RAMP[2]}
    return {lv: RAMP[min(2, round(i * 2 / (len(levels) - 1)))] for i, lv in enumerate(levels)}


def _pct(v, _pos=None):
    return "0%" if abs(v) < 1e-9 else f"{v:+g}%"


def _even_k_scale(ks):
    """Axis functions that put the tested k values at evenly spaced positions.

    On a plain log axis 1 -> 64 takes most of the width and 256, 512 and 1024
    (where the effect is) crowd the right edge; here every tested k gets the
    same room. Between and beyond the tested values the scale is log2-linear.
    """
    import numpy as np

    lk = np.log2(np.asarray(ks, dtype=float))
    idx = np.arange(len(lk), dtype=float)

    def forward(x):
        p = np.log2(np.maximum(np.asarray(x, dtype=float), 1e-9))
        y = np.interp(p, lk, idx)
        y = np.where(p < lk[0], (p - lk[0]) / (lk[1] - lk[0]), y)
        return np.where(p > lk[-1], idx[-1] + (p - lk[-1]) / (lk[-1] - lk[-2]), y)

    def inverse(y):
        y = np.asarray(y, dtype=float)
        p = np.interp(y, idx, lk)
        p = np.where(y < 0, lk[0] + y * (lk[1] - lk[0]), p)
        p = np.where(y > idx[-1], lk[-1] + (y - idx[-1]) * (lk[-1] - lk[-2]), p)
        return 2 ** p

    return forward, inverse


def _style_axes(ax, ks=None, zero=False, pct=False, y_from_zero=False):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.6, linestyle="-")
    ax.set_axisbelow(True)
    ax.tick_params(length=3, width=0.6)
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5, steps=[1, 2, 5, 10], min_n_ticks=3))
    if ks:
        ks = sorted(set(ks))
        if len(ks) >= 2:
            forward, inverse = _even_k_scale(ks)
            ax.set_xscale("function", functions=(forward, inverse))
            ax.set_xlim(float(inverse(-0.35)), float(inverse(len(ks) - 1 + 0.35)))
        else:
            ax.set_xscale("log", base=2)
        ax.set_xticks(ks)
        ax.xaxis.set_minor_locator(NullLocator())
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(round(v))}"))
    if zero:
        ax.axhline(0, color=AXIS, linewidth=0.9, zorder=1)
    if pct:
        ax.yaxis.set_major_formatter(FuncFormatter(_pct))


def _finish(fig, path_base, title, subtitle, legend=None):
    """Header (title, subtitle, legend row) in a band at the top, then save.

    The subtitle wraps to the figure width; the band grows by one line per
    extra subtitle line, and the figure grows with it, so nothing overlaps.
    """
    import textwrap

    sub_lines = textwrap.wrap(subtitle or "", width=max(40, int(fig.get_figwidth() * 15.5)))
    extra = 0.17 * max(0, len(sub_lines) - 1)
    if extra:
        fig.set_figheight(fig.get_figheight() + extra)
    h = fig.get_figheight()
    header = (HEADER_WITH_LEGEND_IN if legend else HEADER_IN) + extra
    fig.tight_layout(rect=(0, 0, 1, 1 - header / h))
    fig.text(0.012, 1 - 0.14 / h, title, ha="left", va="top", fontsize=11.5, color=INK)
    if sub_lines:
        fig.text(0.012, 1 - 0.40 / h, "\n".join(sub_lines), ha="left", va="top", fontsize=8.5, color=INK2,
                 linespacing=1.35)
    if legend:
        handles, labels = legend
        fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.006, 1 - (0.58 + extra) / h),
                   ncol=len(handles), fontsize=8.5, handlelength=2.0, columnspacing=1.6, borderaxespad=0)
    fig.savefig(path_base + ".png", dpi=200)
    fig.savefig(path_base + ".pdf")
    plt.close(fig)


def _line_key(color):
    return Line2D([], [], color=color, linewidth=1.8, marker="o", markersize=5.5,
                  markeredgecolor=SURFACE, markeredgewidth=1.2)


def _plot_series(ax, s, color):
    pts = [(x, y) for x, y in zip(s["x"], s["y"]) if y is not None]
    if not pts:
        return None
    ax.plot([p[0] for p in pts], [p[1] for p in pts], color=color, linewidth=1.8, marker="o", markersize=5.5,
            markeredgecolor=SURFACE, markeredgewidth=1.2, solid_capstyle="round", zorder=3)
    band = [(x, lo, hi) for x, lo, hi in zip(s["x"], s.get("lo", []), s.get("hi", []))
            if lo is not None and hi is not None]
    if len(band) >= 2:
        ax.fill_between([b[0] for b in band], [b[1] for b in band], [b[2] for b in band],
                        color=color, alpha=0.10, linewidth=0, zorder=2)
    return pts[-1]


def change_grid(path_base, title, subtitle, cells, ks, ylabel, row_titles=None, col_titles=None,
                direct_labels=True, sharey="all"):
    """Grid of '% change vs the smallest k' panels.

    cells[r][c] = list of series {"level", "label", "x", "y", "lo", "hi"}.
    sharey="all": every panel on one scale (cold vs warm compare directly);
    sharey="col": one scale per column (one measure per column).
    """
    nrows, ncols = len(cells), len(cells[0])
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.9 * ncols + 0.7, 2.75 * nrows + HEADER_WITH_LEGEND_IN + 0.35),
                             sharex=True, sharey=sharey, squeeze=False)
    levels = [s["level"] for row in cells for cell in row for s in cell]
    colors = level_colors(levels) if levels else {}
    keys = {}
    ends_per_ax = []
    for r in range(nrows):
        for c in range(ncols):
            ax = axes[r][c]
            _style_axes(ax, ks, zero=True, pct=True)
            series = cells[r][c]
            ends = []
            for s in sorted(series, key=lambda s: s["level"]):
                end = _plot_series(ax, s, colors[s["level"]])
                if end:
                    ends.append((end, s["label"]))
                keys.setdefault(s["level"], s["label"])
            if not ends:
                ax.text(0.5, 0.5, "no data", transform=ax.transAxes, ha="center", va="center", color=MUTED)
            ends_per_ax.append((ax, ends))
            if r == 0 and col_titles:
                ax.set_title(col_titles[c], loc="left")
            if c == 0:
                ax.set_ylabel((row_titles[r] + "\n" if row_titles else "") + ylabel)
            if r == nrows - 1:
                ax.set_xlabel("k (virtual disks per DataNode)")
    # Direct labels once the shared scales are final; skipped where ends collide.
    if direct_labels:
        for ax, ends in ends_per_ax:
            if not ends:
                continue
            ymin, ymax = ax.get_ylim()
            yvals = sorted(e[0][1] for e in ends)
            if all(b - a >= (ymax - ymin) * 0.08 for a, b in zip(yvals, yvals[1:])):
                for (x, y), label in ends:
                    ax.annotate(label, (x, y), xytext=(7, 0), textcoords="offset points",
                                va="center", ha="left", fontsize=8, color=INK2, annotation_clip=False)
    legend = None
    if len(keys) >= 2:
        order = sorted(keys)
        legend = ([_line_key(colors[lv]) for lv in order], [keys[lv] for lv in order])
    _finish(fig, path_base, title, subtitle, legend)


def controls(path_base, title, subtitle, panels):
    """Dot plot: runtime change at the largest k per variant, one panel per cache mode.

    panels = [(panel_title, [(variant_label, pct, lo, hi), ...]), ...]
    """
    labels = []
    for _, rows in panels:
        for lab, *_ in rows:
            if lab not in labels:
                labels.append(lab)
    n = len(panels)
    fig, axes = plt.subplots(1, n, figsize=(3.9 * n + 2.2, 0.62 * len(labels) + 1.3 + HEADER_IN),
                             sharex=True, squeeze=False)
    for i, (ptitle, rows) in enumerate(panels):
        ax = axes[0][i]
        for side in ("top", "right", "left"):
            ax.spines[side].set_visible(False)
        ax.grid(axis="x", color=GRID, linewidth=0.6)
        ax.set_axisbelow(True)
        ax.axvline(0, color=AXIS, linewidth=0.9, zorder=1)
        ax.xaxis.set_major_locator(MaxNLocator(nbins=5, steps=[1, 2, 5, 10]))
        ax.xaxis.set_major_formatter(FuncFormatter(_pct))
        ax.tick_params(axis="y", length=0)
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels if i == 0 else [""] * len(labels))
        ax.set_ylim(len(labels) - 0.5, -0.6)
        for lab, pct, lo, hi in rows:
            if pct is None:
                continue
            y = labels.index(lab)
            if lo is not None and hi is not None:
                ax.plot([lo, hi], [y, y], color=BLUE, linewidth=1.8, solid_capstyle="round", zorder=2)
            ax.plot([pct], [y], marker="o", markersize=7, color=BLUE, markeredgecolor=SURFACE,
                    markeredgewidth=1.4, zorder=3)
            ax.annotate(f"{pct:+.1f}%", (pct, y), xytext=(0, 7), textcoords="offset points",
                        ha="center", fontsize=8, color=INK2)
        ax.set_title(ptitle, loc="left")
        ax.set_xlabel("runtime change, k=1 to the largest k")
    all_x = [v for _, rows in panels for _, p, lo, hi in rows for v in (p, lo, hi) if v is not None]
    if all_x:
        lo_x, hi_x = min(all_x + [0]), max(all_x + [0])
        span = (hi_x - lo_x) or 1
        axes[0][0].set_xlim(lo_x - 0.08 * span, hi_x + 0.12 * span)
    _finish(fig, path_base, title, subtitle)


def small_multiples(path_base, title, subtitle, panels, ks, ncols=3):
    """One measure per panel (own y axis, from 0), x = k. panels = [(title, [value per k])]."""
    nrows = math.ceil(len(panels) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.7 * ncols + 0.5, 2.45 * nrows + HEADER_IN + 0.35),
                             sharex=True, squeeze=False)
    for i in range(nrows * ncols):
        ax = axes[i // ncols][i % ncols]
        if i >= len(panels):
            ax.set_visible(False)
            continue
        ptitle, values = panels[i]
        _style_axes(ax, ks)
        pts = [(k, v) for k, v in zip(ks, values) if v is not None]
        if pts:
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=BLUE, linewidth=1.8, marker="o",
                    markersize=5.5, markeredgecolor=SURFACE, markeredgewidth=1.2, zorder=3)
            top = max(p[1] for p in pts)
            ax.set_ylim(0, top * 1.25 if top > 0 else 1)
            k_last, v_last = pts[-1]
            ax.annotate(f"{v_last:,.0f}" if abs(v_last) >= 10 else f"{v_last:.1f}", (k_last, v_last),
                        xytext=(0, 7), textcoords="offset points", ha="center", fontsize=8, color=INK2)
        else:
            ax.text(0.5, 0.5, "no data", transform=ax.transAxes, ha="center", va="center", color=MUTED)
        ax.set_title(ptitle, loc="left")
        if i + ncols >= len(panels):
            ax.set_xlabel("k (virtual disks per DataNode)")
            ax.tick_params(labelbottom=True)
    _finish(fig, path_base, title, subtitle)
