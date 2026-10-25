/* Optional DOM integration test: npm install --no-save jsdom@26.1.0
 * Run against a live API with BASE_URL=http://127.0.0.1:8000 node scripts/verify_dashboard.cjs.
 * This checks JavaScript/DOM/API behavior; it does NOT claim real-browser layout QA.
 */
const {JSDOM, VirtualConsole} = require('jsdom');
const fs = require('node:fs');
const assert = require('node:assert/strict');
const base = process.env.BASE_URL || 'http://127.0.0.1:8000';
const errors = [];
async function waitFor(predicate, label) {
  const end = Date.now() + 20000;
  while (Date.now() < end) {
    if (predicate()) return;
    await new Promise(r=>setTimeout(r,50));
  }
  throw new Error('Timed out: '+label+'; '+errors.join('; '));
}
(async()=>{
  const console = new VirtualConsole();
  console.on('jsdomError',error=>errors.push(error.message));
  const dom = await JSDOM.fromURL(base, {
    runScripts:'dangerously', resources:'usable', pretendToBeVisual:true, virtualConsole:console,
    beforeParse(window){
      window.fetch=(url,opts)=>fetch(new URL(url,base),opts);
      window.HTMLElement.prototype.scrollIntoView=function(){};
      window.HTMLCanvasElement.prototype.getContext=()=>new Proxy({}, {get:()=>()=>{}});
      window.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
    }
  });
  try {
    const d=dom.window.document;
    await waitFor(()=>d.querySelectorAll('#quality-metrics .quality-card').length===4,'four Graph metric cards');
    await waitFor(()=>!d.querySelector('#session-recommend').disabled && d.querySelectorAll('#session-results .product-id').length>0,'initial recommendations');
    assert.match(d.querySelector('#quality-metrics').textContent,/HitRate@10/);
    assert.match(d.querySelector('#model-algorithm').textContent,/Graph/);
    const metadata=await (await fetch(base+'/api/metrics')).json();
    assert.equal(metadata.algorithm,'graph_session');
    const q=await (await fetch(base+'/api/session/recommendations',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({events:[{itemid:129960}],k:10})})).json();
    d.querySelector('#session-item').value='129960';
    d.querySelector('#session-event-form').dispatchEvent(new dom.window.Event('submit',{bubbles:true,cancelable:true}));
    await waitFor(()=>d.querySelector('#session-debug').textContent.includes('1 sự kiện')&&!d.querySelector('#session-recommend').disabled,'manual item interaction');
    assert.match(d.querySelector('#session-debug').textContent,new RegExp(metadata.model_version));
    assert.ok(!q.items.some(r=>r.itemid===129960));
    const contextAfterItem=d.querySelector('#session-context').textContent;
    d.querySelector('#session-results .session-action').click();
    await waitFor(()=>d.querySelector('#session-debug').textContent.includes('2 sự kiện')&&!d.querySelector('#session-recommend').disabled,'add to cart');
    assert.match(d.querySelector('#session-context').textContent,/\+ Giỏ/);
    d.querySelector('#session-results .product-buy').click();
    await waitFor(()=>d.querySelector('#session-debug').textContent.includes('3 sự kiện')&&!d.querySelector('#session-recommend').disabled,'simulated purchase');
    assert.match(d.querySelector('#session-context').textContent,/Mua thử/);
    d.querySelector('#session-clear').click();
    await waitFor(()=>d.querySelector('#session-context').textContent.includes('chưa có tương tác')&&!d.querySelector('#session-recommend').disabled,'new session');
    assert.equal(d.querySelector('#session-error').hidden,true);
    assert.deepEqual(errors,[]);
    const report={status:'passed',kind:'jsdom DOM + live HTTP; no real-browser rendering',
      algorithm:metadata.algorithm,model_version:metadata.model_version,metric_model_version:metadata.metric_model_version,
      checks:['4 metric cards','Graph identity','manual view updates context','add to cart','simulated purchase','seen filtering','reset session'],
      context_after_item:contextAfterItem,errors};
    if(process.env.REPORT_PATH)fs.writeFileSync(process.env.REPORT_PATH,JSON.stringify(report,null,2));
    process.stdout.write(JSON.stringify(report,null,2)+'\n');
  } finally { dom.window.close(); }
})().catch(error=>{process.stderr.write(error.stack+'\n');process.exitCode=1;});
