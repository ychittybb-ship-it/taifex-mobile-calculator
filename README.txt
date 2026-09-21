手機網頁版部署：上傳此資料夾到 GitHub，再用 Render 建立 Web Service。
Build Command: pip install -r requirements.txt
Start Command: gunicorn -w 1 -b 0.0.0.0:$PORT server:app
Environment Variable: FUGLE_API_KEY=你的 Fugle API Key
部署完成後取得 https 網址，手機 Safari 直接開啟並可加入主畫面。
此版本只讀行情，不連接國泰下單。
