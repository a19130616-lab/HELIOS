"""
Reddit Ingestion Module for Helios Trading System
Fetches posts from specified subreddits and performs sentiment analysis.
"""

import logging
import time
import threading
import json
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta

try:
    import praw
    from praw.exceptions import PRAWException
    PRAW_AVAILABLE = True
except ImportError:
    PRAW_AVAILABLE = False
    logging.warning("PRAW not available - Reddit ingestion disabled")

from app.utils import RedisManager, get_timestamp
from app.config_manager import get_config
from app.sentiment_utils import SentimentAnalyzer


class RedditIngestor:
    """
    Reddit data ingestion service that fetches posts from crypto-related subreddits
    and performs sentiment analysis.
    """
    
    def __init__(self, redis_manager: RedisManager):
        """
        Initialize the Reddit Ingestor.
        
        Args:
            redis_manager: Redis manager instance
        """
        self.redis_manager = redis_manager
        self.config = get_config()
        self.logger = logging.getLogger(__name__)
        self.is_running = False
        self.posts_fetched = 0
        self.posts_processed = 0
        self.last_fetch_time = None
        self.fetch_errors = 0
        
        # Reddit configuration
        self.enabled = self.config.get('reddit', 'enabled', bool, False)
        
        # Initialize subreddits as empty list by default
        self.subreddits = []
        
        if not self.enabled:
            self.logger.info("Reddit ingestion disabled in configuration")
            self.reddit = None
            self.sentiment_analyzer = None
            self.sentiment_enabled = False
            return
        
        if not PRAW_AVAILABLE:
            self.logger.error("PRAW not available - cannot initialize Reddit ingestion")
            self.reddit = None
            self.sentiment_analyzer = None
            self.sentiment_enabled = False
            return
        
        # Reddit API credentials
        self.client_id = self.config.get('reddit', 'client_id', str)
        self.client_secret = self.config.get('reddit', 'client_secret', str)
        self.user_agent = self.config.get('reddit', 'user_agent', str)
        
        # Reddit settings
        self.subreddits = self.config.get('reddit', 'subreddits', str).split(',')
        self.poll_interval = self.config.get('reddit', 'poll_interval_sec', int, 120)
        self.max_posts_per_fetch = self.config.get('reddit', 'max_posts_per_fetch', int, 50)
        self.score_min = self.config.get('reddit', 'score_min', int, 5)
        self.hours_lookback = self.config.get('reddit', 'hours_lookback', int, 24)
        
        # Initialize Reddit client
        self.reddit = None
        self.sentiment_analyzer = None
        self._initialize_reddit_client()
        
        # Ingestion state
        self.processed_posts: Set[str] = set()
        
        # Performance tracking
        self.posts_fetched = 0
        self.posts_processed = 0
        self.fetch_errors = 0
        self.last_fetch_time = 0
        
        # Sentiment configuration
        self.sentiment_enabled = self.config.get('sentiment', 'enabled', bool, False)
        if self.sentiment_enabled:
            self.sentiment_analyzer = SentimentAnalyzer()
        
        self.decay_half_life = self.config.get('sentiment', 'decay_half_life_sec', float, 1800.0)
        self.min_posts_window = self.config.get('sentiment', 'min_posts_window', int, 10)
    
    def _initialize_reddit_client(self) -> None:
        """Initialize the Reddit API client."""
        if not PRAW_AVAILABLE or not self.enabled:
            return
        
        try:
            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent,
                check_for_async=False
            )
            
            # Test the connection
            self.reddit.user.me()
            self.logger.info("Reddit API client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Reddit client: {e}")
            self.reddit = None
    
    def start(self) -> None:
        """Start the Reddit ingestion service."""
        if not self.enabled or not self.reddit:
            self.logger.info("Reddit ingestion not available or disabled")
            return
        
        self.logger.info("Starting Reddit Ingestor...")
        self.is_running = True
        
        # Start ingestion thread
        self.ingestion_thread = threading.Thread(target=self._ingestion_loop, daemon=True)
        self.ingestion_thread.start()
        
        # Start sentiment aggregation thread
        if self.sentiment_enabled:
            self.sentiment_thread = threading.Thread(target=self._sentiment_aggregation_loop, daemon=True)
            self.sentiment_thread.start()
        
        self.logger.info("Reddit Ingestor started")
    
    def stop(self) -> None:
        """Stop the Reddit ingestion service."""
        self.logger.info("Stopping Reddit Ingestor...")
        self.is_running = False
        self.logger.info("Reddit Ingestor stopped")
    
    def _ingestion_loop(self) -> None:
        """Main ingestion loop."""
        while self.is_running:
            try:
                self._fetch_and_process_posts()
                time.sleep(self.poll_interval)
            except Exception as e:
                self.logger.error(f"Error in ingestion loop: {e}")
                self.fetch_errors += 1
                time.sleep(60)  # Wait longer on error
    
    def _fetch_and_process_posts(self) -> None:
        """Fetch and process posts from configured subreddits."""
        if not self.reddit:
            return
        
        try:
            current_time = datetime.utcnow()
            cutoff_time = current_time - timedelta(hours=self.hours_lookback)
            
            all_posts = []
            
            # Fetch from each subreddit
            for subreddit_name in self.subreddits:
                subreddit_name = subreddit_name.strip()
                if not subreddit_name:
                    continue
                
                try:
                    subreddit = self.reddit.subreddit(subreddit_name)
                    posts = self._fetch_subreddit_posts(subreddit, cutoff_time)
                    all_posts.extend(posts)
                    
                except Exception as e:
                    self.logger.error(f"Error fetching from r/{subreddit_name}: {e}")
                    self.fetch_errors += 1
            
            # Process and store posts
            self._process_posts(all_posts)
            self.last_fetch_time = get_timestamp()
            
            self.logger.info(f"Fetched and processed {len(all_posts)} posts from Reddit")
            
        except Exception as e:
            self.logger.error(f"Error in fetch and process: {e}")
            self.fetch_errors += 1
    
    def _fetch_subreddit_posts(self, subreddit, cutoff_time: datetime) -> List[Dict]:
        """Fetch posts from a specific subreddit."""
        posts = []
        
        try:
            # Fetch hot posts
            for submission in subreddit.hot(limit=self.max_posts_per_fetch // 2):
                post_data = self._extract_post_data(submission, cutoff_time)
                if post_data:
                    posts.append(post_data)
            
            # Fetch new posts
            for submission in subreddit.new(limit=self.max_posts_per_fetch // 2):
                post_data = self._extract_post_data(submission, cutoff_time)
                if post_data:
                    posts.append(post_data)
            
        except Exception as e:
            self.logger.error(f"Error fetching from subreddit {subreddit.display_name}: {e}")
        
        return posts
    
    def _extract_post_data(self, submission, cutoff_time: datetime) -> Optional[Dict]:
        """Extract relevant data from a Reddit submission."""
        try:
            # Check if post is recent enough
            post_time = datetime.fromtimestamp(submission.created_utc)
            if post_time < cutoff_time:
                return None
            
            # Check if already processed
            if submission.id in self.processed_posts:
                return None
            
            # Check minimum score
            if submission.score < self.score_min:
                return None
            
            # Extract post data
            post_data = {
                'id': submission.id,
                'title': submission.title,
                'selftext': getattr(submission, 'selftext', ''),
                'score': submission.score,
                'upvote_ratio': getattr(submission, 'upvote_ratio', 0.5),
                'num_comments': submission.num_comments,
                'created_utc': submission.created_utc,
                'subreddit': submission.subreddit.display_name,
                'author': str(submission.author) if submission.author else '[deleted]',
                'url': submission.url,
                'permalink': f"https://reddit.com{submission.permalink}",
                'timestamp': get_timestamp()
            }
            
            # Mark as processed
            self.processed_posts.add(submission.id)
            self.posts_fetched += 1
            
            return post_data
            
        except Exception as e:
            self.logger.error(f"Error extracting post data: {e}")
            return None
    
    def _process_posts(self, posts: List[Dict]) -> None:
        """Process posts and perform sentiment analysis."""
        if not posts:
            return
        
        processed_posts = []
        
        for post in posts:
            try:
                # Perform sentiment analysis if enabled
                if self.sentiment_enabled and self.sentiment_analyzer:
                    sentiment_result = self.sentiment_analyzer.analyze_post(
                        title=post['title'],
                        content=post['selftext']
                    )
                    post.update(sentiment_result)
                
                processed_posts.append(post)
                self.posts_processed += 1
                
            except Exception as e:
                self.logger.error(f"Error processing post {post.get('id', 'unknown')}: {e}")
        
        # Store posts in Redis
        self._store_posts(processed_posts)
        
        # Publish news update event
        if processed_posts:
            self.redis_manager.publish("news_updates", {
                'timestamp': get_timestamp(),
                'posts_count': len(processed_posts),
                'source': 'reddit'
            })
    
    def _store_posts(self, posts: List[Dict]) -> None:
        """Store processed posts in Redis."""
        try:
            current_time = get_timestamp()
            
            for post in posts:
                # Store individual post
                post_key = f"reddit:post:{post['id']}"
                self.redis_manager.set_data(post_key, post, expiry=86400)  # 24 hours
                
                # Add to global posts timeline
                timeline_key = "reddit:timeline"
                self.redis_manager.redis_client.zadd(
                    timeline_key, 
                    {json.dumps(post): current_time}
                )
                
                # Trim timeline to keep only recent posts
                cutoff_time = current_time - (self.hours_lookback * 3600 * 1000)
                self.redis_manager.redis_client.zremrangebyscore(timeline_key, 0, cutoff_time)
                
                # Store posts by crypto mentions if sentiment analysis is enabled
                if 'crypto_mentions' in post and post['crypto_mentions']:
                    for symbol in post['crypto_mentions']:
                        symbol_key = f"reddit:symbol:{symbol}"
                        self.redis_manager.redis_client.zadd(
                            symbol_key,
                            {json.dumps(post): current_time}
                        )
                        # Trim symbol-specific timeline
                        self.redis_manager.redis_client.zremrangebyscore(symbol_key, 0, cutoff_time)
            
        except Exception as e:
            self.logger.error(f"Error storing posts in Redis: {e}")
    
    def _sentiment_aggregation_loop(self) -> None:
        """Aggregate sentiment data periodically."""
        while self.is_running:
            try:
                self._aggregate_sentiment_data()
                time.sleep(300)  # Update every 5 minutes
            except Exception as e:
                self.logger.error(f"Error in sentiment aggregation loop: {e}")
                time.sleep(60)
    
    def _aggregate_sentiment_data(self) -> None:
        """Aggregate sentiment data from recent posts."""
        try:
            current_time = datetime.utcnow()
            
            # Get recent posts from timeline
            timeline_key = "reddit:timeline"
            cutoff_timestamp = get_timestamp() - (self.hours_lookback * 3600 * 1000)
            
            recent_posts_data = self.redis_manager.redis_client.zrangebyscore(
                timeline_key, cutoff_timestamp, '+inf'
            )
            
            if not recent_posts_data:
                return
            
            # Parse posts and extract sentiment data
            sentiment_data = []
            for post_json in recent_posts_data:
                try:
                    post = json.loads(post_json)
                    if 'sentiment' in post:
                        sentiment_data.append({
                            'sentiment': post['sentiment'],
                            'direction': post.get('direction', 'neutral'),
                            'timestamp': datetime.fromtimestamp(post['created_utc']),
                            'crypto_mentions': post.get('crypto_mentions', [])
                        })
                except Exception as e:
                    self.logger.error(f"Error parsing post for sentiment: {e}")
            
            if len(sentiment_data) < self.min_posts_window:
                self.logger.warning(f"Insufficient posts for sentiment aggregation: {len(sentiment_data)} < {self.min_posts_window}")
                return
            
            # Aggregate overall sentiment
            overall_sentiment = self.sentiment_analyzer.aggregate_sentiments(
                sentiment_data, self.decay_half_life, current_time
            )
            
            # Store aggregated sentiment
            sentiment_key = "sentiment:global"
            sentiment_summary = {
                'timestamp': get_timestamp(),
                'aggregated_sentiment': overall_sentiment,
                'posts_analyzed': len(sentiment_data),
                'last_updated': current_time.isoformat()
            }
            
            self.redis_manager.set_data(sentiment_key, sentiment_summary, expiry=3600)
            
            # Publish sentiment update
            self.redis_manager.publish("sentiment_updates", sentiment_summary)
            
            # Aggregate sentiment by crypto symbol
            self._aggregate_symbol_sentiment(sentiment_data, current_time)
            
            self.logger.info(f"Aggregated sentiment from {len(sentiment_data)} posts")
            
        except Exception as e:
            self.logger.error(f"Error aggregating sentiment data: {e}")
    
    def _aggregate_symbol_sentiment(self, sentiment_data: List[Dict], current_time: datetime) -> None:
        """Aggregate sentiment data by crypto symbol."""
        try:
            # Group by crypto mentions
            symbol_sentiments = {}
            
            for item in sentiment_data:
                crypto_mentions = item.get('crypto_mentions', [])
                if crypto_mentions:
                    for symbol in crypto_mentions:
                        if symbol not in symbol_sentiments:
                            symbol_sentiments[symbol] = []
                        symbol_sentiments[symbol].append(item)
            
            # Aggregate sentiment for each symbol
            for symbol, items in symbol_sentiments.items():
                if len(items) >= 3:  # Minimum posts for symbol-specific sentiment
                    aggregated = self.sentiment_analyzer.aggregate_sentiments(
                        items, self.decay_half_life, current_time
                    )
                    
                    symbol_sentiment = {
                        'timestamp': get_timestamp(),
                        'symbol': symbol,
                        'aggregated_sentiment': aggregated,
                        'posts_analyzed': len(items),
                        'last_updated': current_time.isoformat()
                    }
                    
                    # Store symbol-specific sentiment
                    sentiment_key = f"sentiment:symbol:{symbol}"
                    self.redis_manager.set_data(sentiment_key, symbol_sentiment, expiry=3600)
            
        except Exception as e:
            self.logger.error(f"Error aggregating symbol sentiment: {e}")
    
    def get_health_status(self) -> Dict[str, any]:
        """
        Get health status of the Reddit Ingestor.
        
        Returns:
            Health status information
        """
        return {
            'is_running': self.is_running,
            'enabled': self.enabled,
            'reddit_client_available': self.reddit is not None,
            'sentiment_enabled': self.sentiment_enabled,
            'posts_fetched': self.posts_fetched,
            'posts_processed': self.posts_processed,
            'fetch_errors': getattr(self, 'fetch_errors', 0),
            'last_fetch_time': self.last_fetch_time,
            'processed_posts_count': len(getattr(self, 'processed_posts', set())),
            'configured_subreddits': self.subreddits if self.enabled else []
        }
    
    def get_recent_sentiment(self, symbol: Optional[str] = None) -> Optional[Dict]:
        """
        Get recent sentiment data.
        
        Args:
            symbol: Optional crypto symbol to get sentiment for
            
        Returns:
            Recent sentiment data or None
        """
        try:
            if symbol:
                sentiment_key = f"sentiment:symbol:{symbol}"
            else:
                sentiment_key = "sentiment:global"
            
            return self.redis_manager.get_data(sentiment_key)
            
        except Exception as e:
            self.logger.error(f"Error getting recent sentiment: {e}")
            return None