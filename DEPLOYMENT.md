# Deployment Guide - Web Access for Boss

## Quick Options for Web Access

### Option 1: Railway (Easiest - 5 minutes)
1. Go to [railway.app](https://railway.app)
2. Sign up with GitHub
3. Click "New Project" → "Deploy from GitHub repo"
4. Select your `walmartscraper` repository
5. Railway will automatically detect the Dockerfile and deploy
6. Get a public URL like: `https://walmartscraper-production.up.railway.app`

### Option 2: Render (Free tier available)
1. Go to [render.com](https://render.com)
2. Connect GitHub account
3. Create "Web Service" from your repo
4. Set build command: `docker build -t walmartscraper .`
5. Set start command: `docker run -p 8000:8000 walmartscraper python walmart/api.py`
6. Get URL: `https://your-app-name.onrender.com`

### Option 3: Heroku (Paid)
1. Install Heroku CLI
2. `heroku create walmartscraper-api`
3. `heroku container:push web`
4. `heroku container:release web`
5. Get URL: `https://walmartscraper-api.herokuapp.com`

### Option 4: ngrok (Local testing - Immediate)
```bash
# Install ngrok
# Download from https://ngrok.com/download

# Start your API locally
python walmart/api.py

# In another terminal, expose it
ngrok http 8000

# Get public URL like: https://abc123.ngrok.io
```

## Environment Setup for Production

### 1. Set Environment Variables
In your cloud platform, set these environment variables:
```
BLUECART_API_KEY=your_actual_api_key
WALMART_DOMAIN=walmart.com
OUTPUT_DIR=/tmp/output
DATABASE_PATH=/tmp/walmart.db
```

### 2. Update Dockerfile for Production
```dockerfile
# Add to your Dockerfile
EXPOSE 8000
CMD ["python", "walmart/api.py"]
```

## API Usage for Boss

Once deployed, your boss can access:

### Web Interface
- **API Docs**: `https://your-app-url/docs`
- **Health Check**: `https://your-app-url/health`

### API Endpoints
```bash
# Start scraping
curl -X POST "https://your-app-url/scrape" \
  -H "Content-Type: application/json" \
  -d '{"keywords": "nike", "max_per_keyword": 10}'

# Check status
curl "https://your-app-url/tasks/task_1234567890"

# Download results
curl "https://your-app-url/latest"
```

### Simple Web Interface
You can also create a simple HTML form for your boss:

```html
<!DOCTYPE html>
<html>
<head><title>Walmart Scraper</title></head>
<body>
    <h1>Walmart Scraper</h1>
    <form id="scrapeForm">
        <input type="text" id="keywords" placeholder="Keywords (e.g., nike,adidas)" required>
        <input type="number" id="maxPerKeyword" value="10" min="1" max="100">
        <button type="submit">Start Scraping</button>
    </form>
    <div id="result"></div>
    
    <script>
        document.getElementById('scrapeForm').onsubmit = async (e) => {
            e.preventDefault();
            const response = await fetch('/scrape', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    keywords: document.getElementById('keywords').value,
                    max_per_keyword: parseInt(document.getElementById('maxPerKeyword').value)
                })
            });
            const data = await response.json();
            document.getElementById('result').innerHTML = 
                `Task started: ${data.task_id}<br>Status: ${data.status}`;
        };
    </script>
</body>
</html>
```

## Security Considerations

1. **API Key Protection**: Never commit your `.env` file
2. **Rate Limiting**: Consider adding rate limiting for production
3. **Authentication**: Add basic auth for sensitive operations
4. **HTTPS**: All cloud platforms provide HTTPS by default

## Monitoring

- Check logs in your cloud platform dashboard
- Monitor API usage and costs
- Set up alerts for failures
