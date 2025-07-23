from fastapi import FastAPI
import scraper  # this is your existing scraper.py

app = FastAPI(title="Flashscore Scraper API")

# Simple example � adapt to your scraper's functions
@app.get("/scrape")
def run_scrape():
    return scraper.main()  # or whatever entry-point you have