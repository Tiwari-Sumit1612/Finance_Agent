import sys
import os
import asyncio

# Append the project root directory to sys.path to resolve the 'ingestion' module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.streaming import MarketFeedStream, NewsFeedStream

async def main():
    print("Starting Streaming Ingestion Feeds concurrently...")
    market_stream = MarketFeedStream()
    news_stream = NewsFeedStream()
    
    # Run both data feeds concurrently inside the async event loop
    await asyncio.gather(
        market_stream.run_stream(),
        news_stream.start_polling()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Streaming engines stopped cleanly by user request.")