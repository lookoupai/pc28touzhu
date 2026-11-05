const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {spawn} = require('node:child_process');
const {chromium} = require(path.join(process.env.PC28_QA_NODE_MODULES || '/tmp/pc28-cycle-ui-tools/node_modules','playwright-core'));
const root=path.resolve(__dirname,'../../..');
const evidence=path.join(root,'.boss/cycle-isolation/evidence');
const child=spawn('python3',['tests/cycle_ui_server.py'],{cwd:root,stdio:['pipe','pipe','pipe'],env:{...process.env,PYTHONPATH:'src'}});
let errors=''; child.stderr.on('data',b=>errors+=b);
(async()=>{
 let line=''; const info=await new Promise((resolve,reject)=>{child.stdout.on('data',b=>{line+=b; if(line.includes('\n')) resolve(JSON.parse(line.split('\n')[0]));});child.on('exit',c=>reject(new Error('fixture exited '+c+' '+errors)));});
 const health=await fetch(info.url+'/api/health'); assert.equal(health.status,200); assert.deepEqual(await health.json(),{status:'ok'});
 const browser=await chromium.launch({executablePath:process.env.PC28_QA_CHROMIUM || '/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless:true,args:['--no-sandbox']});
 const timings=[]; const output=[];
 try {
  for(const width of [390,1440]){
   const context=await browser.newContext({viewport:{width,height:900}});
   const split=info.cookie.indexOf('='); await context.addCookies([{name:info.cookie.slice(0,split),value:info.cookie.slice(split+1),url:info.url}]);
   await context.route('**/*',route=>route.request().url().startsWith(info.url)?route.continue():route.abort());
   const page=await context.newPage(); const pageErrors=[]; page.on('pageerror',e=>pageErrors.push(e.message));
   for(const [slug,url] of [['rules','/auto-triggers'],['manual','/autobet/subscriptions']]){
    const begin=Date.now(); await page.goto(info.url+url,{waitUntil:'networkidle'}); timings.push(Date.now()-begin);
    if(slug==='rules'){
     await page.getByRole('button',{name:'新建',exact:true}).waitFor();
     await page.getByRole('button',{name:'新建',exact:true}).click();
     assert.equal(await page.locator('[name="cycle_max_starts"]').inputValue(),'1');
     assert.equal(await page.locator('[name="cycle_on_target"]').inputValue(),'wait_condition');
    } else {
     await page.getByText('尚未手动开始。',{exact:true}).waitFor();
    }
    const sizes=await page.evaluate(()=>({scroll:document.documentElement.scrollWidth,client:document.documentElement.clientWidth}));
    assert.ok(sizes.scroll<=sizes.client+1,`${slug} ${width} overflow ${JSON.stringify(sizes)}`);
    assert.deepEqual(pageErrors,[]);
    await page.screenshot({path:path.join(evidence,`qa-${slug}-${width}.png`),fullPage:true});
    output.push({page:slug,width,overflow:false,pageErrors:0});
   }
   await context.close();
  }
  console.log(JSON.stringify({health:'ok',checks:output.length,passed:output.length,failed:0,skipped:0,cases:output,load_ms:timings},null,2));
 } finally {await browser.close();child.stdin.end();}
 await new Promise((resolve,reject)=>child.on('exit',c=>c===0?resolve():reject(new Error('fixture cleanup failed '+c+' '+errors))));
})().catch(e=>{console.error(e);child.stdin.end();child.kill();process.exitCode=1;});
