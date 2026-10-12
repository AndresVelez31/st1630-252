// Renderiza un .drawio con mxGraph en Chromium y exporta PNG (2x) y SVG.
const { chromium } = require('playwright-core');
const fs = require('fs');
const path = require('path');

const [drawio, outPng, outSvg] = process.argv.slice(2);
const xml = fs.readFileSync(drawio, 'utf8');
const model = xml.match(/<mxGraphModel[\s\S]*<\/mxGraphModel>/)[0];
const W = +model.match(/pageWidth="(\d+)"/)[1];
const H = +model.match(/pageHeight="(\d+)"/)[1];
const mx = fs.readFileSync(require.resolve('mxgraph/javascript/mxClient.min.js'), 'utf8');

const html = `<!doctype html><html><head><meta charset="utf-8">
<style>body{margin:0;background:#fff} #g{width:${W}px;height:${H}px;overflow:hidden;position:relative}
*{font-family:Helvetica,"Liberation Sans",FreeSans,Arial,sans-serif}</style>
<script>window.mxBasePath='';window.mxLoadResources=false;window.mxLoadStylesheets=false;window.mxForceIncludes=false;</script>
<script>${mx}</script></head><body><div id="g"></div>
<script>
const graph = new mxGraph(document.getElementById('g'));
graph.setHtmlLabels(true); graph.setEnabled(false);
const doc = mxUtils.parseXml(${JSON.stringify(model)});
new mxCodec(doc).decode(doc.documentElement, graph.getModel());
window.done = true;
</script></body></html>`;

(async () => {
  const browser = await chromium.launch(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {});
  const page = await browser.newPage({ viewport: { width: W, height: H }, deviceScaleFactor: 2 });
  await page.setContent(html);
  await page.waitForFunction(() => window.done);
  await page.waitForTimeout(300);
  await page.locator('#g').screenshot({ path: outPng });
  const svg = await page.evaluate(([w, h]) => {
    const s = document.querySelector('#g svg').cloneNode(true);
    s.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
    s.setAttribute('width', w); s.setAttribute('height', h); s.setAttribute('viewBox', `0 0 ${w} ${h}`);
    s.style.background = '#ffffff';
    return new XMLSerializer().serializeToString(s);
  }, [W, H]);
  fs.writeFileSync(outSvg, '<?xml version="1.0" encoding="UTF-8"?>\n' + svg);
  await browser.close();
  console.log('render ok', W, H);
})();
