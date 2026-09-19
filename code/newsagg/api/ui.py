"""Minimal single-page feed viewer, served at ``/``."""

from __future__ import annotations

DEMO_HTML = """
<!doctype html><meta charset="utf-8"><title>News Aggregator</title>
<style>
 :root{color-scheme:light dark}
 body{font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
      max-width:820px;margin:2rem auto;padding:0 1rem}
 h1{font-size:1.4rem;margin-bottom:.25rem}
 .sub{opacity:.65;font-size:.85rem;margin-bottom:1.5rem}
 .story{border:1px solid #8883;border-radius:10px;padding:.9rem 1rem;margin:.7rem 0}
 .t{font-weight:600}
 .meta{font-size:.78rem;opacity:.75;margin-top:.35rem}
 .pill{display:inline-block;border:1px solid #8885;border-radius:999px;
       padding:.05rem .5rem;margin-right:.35rem;font-size:.72rem}
 button{padding:.45rem .9rem;border-radius:8px;border:1px solid #8886;
        background:transparent;cursor:pointer;font:inherit}
 .src{font-size:.75rem;opacity:.6;margin-top:.3rem}
</style>
<h1>News Aggregator</h1>
<div class="sub">Stories ranked by independent source count, authority, velocity and recency.</div>
<button onclick="load()">Refresh</button>
<div id="out"></div>
<script>
async function load(){
  const r = await fetch('/v1/stories?limit=25');
  const d = await r.json();
  document.getElementById('out').innerHTML = d.stories.map(s => `
    <div class="story">
      <div class="t">${s.title}</div>
      <div class="meta">
        <span class="pill">score ${s.score.toFixed(4)}</span>
        <span class="pill">${s.signals.independent_sources} independent</span>
        <span class="pill">${s.signals.syndicated_sources} syndicated</span>
        <span class="pill">${s.signals.articles} articles</span>
        <span class="pill">recency x${s.signals.recency_multiplier}</span>
        ${(s.topics||[]).map(t=>`<span class="pill">${t}</span>`).join('')}
      </div>
      <div class="src">${(s.sources||[]).map(x=>x.name+(x.independent?'':' (wire)')).join(' \\u00b7 ')}</div>
    </div>`).join('') || '<p>No stories yet. Run: <code>python main.py demo</code></p>';
}
load();
</script>
"""
