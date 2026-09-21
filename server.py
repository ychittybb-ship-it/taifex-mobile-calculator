import os,json,asyncio,threading
from flask import Flask,send_file,jsonify
from dotenv import load_dotenv
import websockets
load_dotenv()
KEY=os.getenv('FUGLE_API_KEY','').strip(); PORT=int(os.getenv('PORT','8080'))
app=Flask(__name__); state={'price':None,'symbol':'TXF1!','connected':False,'last_update':None,'error':''}
URL='wss://api.fugle.tw/marketdata/v1.1/futopt/streaming'
async def market_loop():
    while True:
        if not KEY:
            state['error']='尚未設定 FUGLE_API_KEY'; await asyncio.sleep(5); continue
        try:
            async with websockets.connect(URL,ping_interval=20,ping_timeout=20) as ws:
                await ws.send(json.dumps({'event':'auth','data':{'apikey':KEY}}))
                await ws.send(json.dumps({'event':'subscribe','data':{'channel':'trades','symbol':'TXF1!','afterHours':True}}))
                state['connected']=True; state['error']=''
                async for raw in ws:
                    try:
                        m=json.loads(raw); d=m.get('data',{}); ts=d.get('trades',[]) if isinstance(d,dict) else []
                        if ts and not ts[0].get('isTrial',False) and ts[0].get('price') is not None:
                            state['price']=float(ts[0]['price']); state['last_update']=ts[0].get('timestamp'); state['symbol']=d.get('symbol','TXF1!')
                    except Exception: pass
        except Exception as e:
            state['connected']=False; state['error']=str(e)[:180]; await asyncio.sleep(5)
@app.get('/')
def home(): return send_file('index.html')
@app.get('/api/quote')
def quote(): return jsonify(state)
@app.get('/health')
def health(): return jsonify({'ok':True,'market_connected':state['connected']})
if __name__=='__main__':
    threading.Thread(target=lambda:asyncio.run(market_loop()),daemon=True).start(); app.run(host='0.0.0.0',port=PORT)
