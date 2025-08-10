"""
Sentiment Analysis Utilities for Helios Trading System
Provides sentiment scoring using VADER and text preprocessing.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False
    logging.warning("VADER sentiment analyzer not available")

try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    NLTK_AVAILABLE = True
    
    # Download required NLTK data
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        nltk.download('punkt', quiet=True)
    
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords', quiet=True)
        
except ImportError:
    NLTK_AVAILABLE = False
    logging.warning("NLTK not available for text preprocessing")


class SentimentAnalyzer:
    """
    Sentiment analyzer using VADER sentiment analysis.
    Provides crypto-specific sentiment scoring with filtering and preprocessing.
    """
    
    def __init__(self):
        """Initialize the sentiment analyzer."""
        self.logger = logging.getLogger(__name__)
        
        if VADER_AVAILABLE:
            self.analyzer = SentimentIntensityAnalyzer()
            
            # Add crypto-specific lexicon updates
            crypto_lexicon = {
                'moon': 2.0, 'mooning': 2.5, 'lambo': 2.0, 'hodl': 1.5,
                'btfd': 1.5, 'diamond hands': 2.0, 'ath': 1.5, 'pump': 2.0,
                'bull': 1.5, 'bullish': 2.0, 'bear': -1.5, 'bearish': -2.0,
                'dump': -2.5, 'rekt': -2.5, 'crash': -2.0, 'fud': -1.5,
                'fomo': 1.0, 'bag holder': -1.5, 'shitcoin': -2.0,
                'scam': -3.0, 'rug pull': -3.0, 'paper hands': -1.5,
                'dip': -1.0, 'buying the dip': 1.5, 'to the moon': 2.5,
                'rocket': 2.0, 'green': 1.5, 'red': -1.5, 'profit': 2.0,
                'loss': -2.0, 'gains': 2.0, 'rally': 2.0, 'correction': -1.0
            }
            
            # Update VADER lexicon with crypto terms
            self.analyzer.lexicon.update(crypto_lexicon)
            self.logger.info("VADER sentiment analyzer initialized with crypto lexicon")
        else:
            self.analyzer = None
            self.logger.warning("VADER not available - sentiment analysis disabled")
        
        # Crypto symbol patterns
        self.crypto_patterns = [
            r'\b(?:BTC|BITCOIN)\b',
            r'\b(?:ETH|ETHEREUM)\b',
            r'\b(?:ADA|CARDANO)\b',
            r'\b(?:DOT|POLKADOT)\b',
            r'\b(?:LINK|CHAINLINK)\b',
            r'\b(?:SOL|SOLANA)\b',
            r'\b(?:AVAX|AVALANCHE)\b',
            r'\b(?:MATIC|POLYGON)\b',
            r'\b(?:UNI|UNISWAP)\b',
            r'\b(?:DOGE|DOGECOIN)\b'
        ]
        
        # Stop words for filtering
        if NLTK_AVAILABLE:
            try:
                self.stop_words = set(stopwords.words('english'))
            except:
                self.stop_words = set()
        else:
            self.stop_words = set()
    
    def preprocess_text(self, text: str) -> str:
        """
        Preprocess text for sentiment analysis.
        
        Args:
            text: Raw text to preprocess
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove Reddit formatting
        text = re.sub(r'/u/\w+', '', text)  # Remove user mentions
        text = re.sub(r'/r/\w+', '', text)  # Remove subreddit mentions
        text = re.sub(r'\*{1,2}([^*]+)\*{1,2}', r'\1', text)  # Remove bold/italic
        
        # Remove excessive punctuation
        text = re.sub(r'[!]{2,}', '!', text)
        text = re.sub(r'[?]{2,}', '?', text)
        text = re.sub(r'[.]{2,}', '...', text)
        
        # Remove emojis (basic)
        text = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U00002700-\U000027BF]', '', text)
        
        # Clean whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def extract_crypto_mentions(self, text: str) -> List[str]:
        """
        Extract cryptocurrency mentions from text.
        
        Args:
            text: Text to analyze
            
        Returns:
            List of detected crypto symbols
        """
        mentions = []
        text_upper = text.upper()
        
        for pattern in self.crypto_patterns:
            matches = re.findall(pattern, text_upper)
            mentions.extend(matches)
        
        # Normalize to standard symbols
        symbol_map = {
            'BITCOIN': 'BTC', 'ETHEREUM': 'ETH', 'CARDANO': 'ADA',
            'POLKADOT': 'DOT', 'CHAINLINK': 'LINK', 'SOLANA': 'SOL',
            'AVALANCHE': 'AVAX', 'POLYGON': 'MATIC', 'UNISWAP': 'UNI',
            'DOGECOIN': 'DOGE'
        }
        
        normalized = []
        for mention in mentions:
            normalized.append(symbol_map.get(mention, mention))
        
        return list(set(normalized))  # Remove duplicates
    
    def analyze_sentiment(self, text: str) -> Dict[str, float]:
        """
        Analyze sentiment of text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment scores
        """
        if not self.analyzer or not text:
            return {
                'compound': 0.0,
                'positive': 0.0,
                'neutral': 1.0,
                'negative': 0.0,
                'confidence': 0.0
            }
        
        try:
            # Preprocess text
            clean_text = self.preprocess_text(text)
            
            if not clean_text:
                return {
                    'compound': 0.0,
                    'positive': 0.0,
                    'neutral': 1.0,
                    'negative': 0.0,
                    'confidence': 0.0
                }
            
            # Get VADER scores
            scores = self.analyzer.polarity_scores(clean_text)
            
            # Calculate confidence based on the distance from neutral
            confidence = abs(scores['compound'])
            
            return {
                'compound': scores['compound'],
                'positive': scores['pos'],
                'neutral': scores['neu'],
                'negative': scores['neg'],
                'confidence': confidence
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing sentiment: {e}")
            return {
                'compound': 0.0,
                'positive': 0.0,
                'neutral': 1.0,
                'negative': 0.0,
                'confidence': 0.0
            }
    
    def analyze_post(self, title: str, content: str = "") -> Dict[str, any]:
        """
        Analyze sentiment of a Reddit post.
        
        Args:
            title: Post title
            content: Post content (optional)
            
        Returns:
            Dictionary with sentiment analysis results
        """
        # Combine title and content, giving more weight to title
        combined_text = f"{title} {title} {content}".strip()
        
        # Get sentiment scores
        sentiment = self.analyze_sentiment(combined_text)
        
        # Extract crypto mentions
        crypto_mentions = self.extract_crypto_mentions(combined_text)
        
        # Determine overall sentiment direction
        compound = sentiment['compound']
        if compound >= 0.3:
            direction = 'positive'
        elif compound <= -0.3:
            direction = 'negative'
        else:
            direction = 'neutral'
        
        return {
            'sentiment': sentiment,
            'direction': direction,
            'crypto_mentions': crypto_mentions,
            'text_length': len(combined_text),
            'has_crypto_content': len(crypto_mentions) > 0
        }
    
    def aggregate_sentiments(self, sentiments: List[Dict[str, any]], 
                           decay_half_life: float = 1800.0,
                           current_time: Optional[datetime] = None) -> Dict[str, float]:
        """
        Aggregate multiple sentiment scores with time decay.
        
        Args:
            sentiments: List of sentiment dictionaries with timestamps
            decay_half_life: Half-life for time decay in seconds
            current_time: Current time for decay calculation
            
        Returns:
            Aggregated sentiment scores
        """
        if not sentiments:
            return {
                'compound_mean': 0.0,
                'compound_std': 0.0,
                'positive_ratio': 0.0,
                'negative_ratio': 0.0,
                'neutral_ratio': 0.0,
                'post_count': 0,
                'confidence_mean': 0.0
            }
        
        if current_time is None:
            current_time = datetime.utcnow()
        
        weighted_compounds = []
        weighted_confidences = []
        directions = []
        total_weight = 0.0
        
        for item in sentiments:
            # Get timestamp
            if 'timestamp' in item:
                if isinstance(item['timestamp'], datetime):
                    post_time = item['timestamp']
                else:
                    post_time = datetime.fromtimestamp(item['timestamp'])
            else:
                post_time = current_time
            
            # Calculate time decay weight
            time_diff = (current_time - post_time).total_seconds()
            weight = 0.5 ** (time_diff / decay_half_life)
            
            # Weight by confidence if available
            if 'sentiment' in item:
                sentiment = item['sentiment']
                confidence = sentiment.get('confidence', 1.0)
                compound = sentiment.get('compound', 0.0)
                
                final_weight = weight * confidence
                weighted_compounds.append(compound * final_weight)
                weighted_confidences.append(confidence * final_weight)
                total_weight += final_weight
            
            # Count directions
            if 'direction' in item:
                directions.append(item['direction'])
        
        if total_weight == 0:
            return {
                'compound_mean': 0.0,
                'compound_std': 0.0,
                'positive_ratio': 0.0,
                'negative_ratio': 0.0,
                'neutral_ratio': 0.0,
                'post_count': len(sentiments),
                'confidence_mean': 0.0
            }
        
        # Calculate weighted averages
        compound_mean = sum(weighted_compounds) / total_weight
        confidence_mean = sum(weighted_confidences) / total_weight
        
        # Calculate standard deviation
        if len(weighted_compounds) > 1:
            variance = sum((c/total_weight - compound_mean)**2 for c in weighted_compounds) / len(weighted_compounds)
            compound_std = variance ** 0.5
        else:
            compound_std = 0.0
        
        # Calculate direction ratios
        total_posts = len(directions)
        positive_count = directions.count('positive')
        negative_count = directions.count('negative')
        neutral_count = directions.count('neutral')
        
        return {
            'compound_mean': compound_mean,
            'compound_std': compound_std,
            'positive_ratio': positive_count / total_posts if total_posts > 0 else 0.0,
            'negative_ratio': negative_count / total_posts if total_posts > 0 else 0.0,
            'neutral_ratio': neutral_count / total_posts if total_posts > 0 else 0.0,
            'post_count': total_posts,
            'confidence_mean': confidence_mean
        }


def create_sentiment_analyzer() -> SentimentAnalyzer:
    """Factory function to create sentiment analyzer."""
    return SentimentAnalyzer()