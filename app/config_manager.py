"""
Configuration Manager for Helios Trading System
Handles loading and validation of configuration parameters.
"""

import configparser
import os
from typing import Dict, Any
import logging

class ConfigManager:
    """Centralized configuration management for the Helios system."""
    
    def __init__(self, config_path: str = "config/config.ini"):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self._load_config()
        self._validate_config()
    
    def _load_config(self) -> None:
        """Load configuration from file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        self.config.read(self.config_path)
        logging.info(f"Configuration loaded from {self.config_path}")
    
    def _validate_config(self) -> None:
        """Validate required configuration sections and keys."""
        required_sections = ['binance', 'trading', 'signals', 'risk', 'system']
        
        for section in required_sections:
            if not self.config.has_section(section):
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate critical parameters
        if self.get('risk', 'kelly_fraction', float) <= 0 or self.get('risk', 'kelly_fraction', float) > 1:
            raise ValueError("Kelly fraction must be between 0 and 1")
        
        if self.get('signals', 'nobi_trigger_threshold', float) <= 0 or self.get('signals', 'nobi_trigger_threshold', float) >= 1:
            raise ValueError("NOBI trigger threshold must be between 0 and 1")
    
    def get(self, section: str, key: str, value_type: type = str, fallback: Any = None) -> Any:
        """
        Get a configuration value with type conversion.
        
        Args:
            section: Configuration section
            key: Configuration key
            value_type: Type to convert the value to
            fallback: Default value if key is not found
            
        Returns:
            Configuration value converted to specified type
        """
        try:
            value = self.config.get(section, key)
            
            if value_type == bool:
                return self.config.getboolean(section, key)
            elif value_type == int:
                return self.config.getint(section, key)
            elif value_type == float:
                return self.config.getfloat(section, key)
            else:
                return value
                
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            if fallback is not None:
                return fallback
            raise KeyError(f"Configuration key not found: {section}.{key}") from e
    
    def get_section(self, section: str) -> Dict[str, str]:
        """
        Get all key-value pairs from a configuration section.
        
        Args:
            section: Configuration section name
            
        Returns:
            Dictionary of configuration values
        """
        if not self.config.has_section(section):
            raise KeyError(f"Configuration section not found: {section}")
        
        return dict(self.config[section])
    
    def update_config(self, section: str, key: str, value: str) -> None:
        """
        Update a configuration value (runtime only, not saved to file).
        
        Args:
            section: Configuration section
            key: Configuration key
            value: New value
        """
        if not self.config.has_section(section):
            self.config.add_section(section)
        
        self.config.set(section, key, str(value))
        logging.info(f"Updated config: {section}.{key} = {value}")


# Global configuration instance
config = None

def get_config() -> ConfigManager:
    """Get the global configuration instance."""
    global config
    if config is None:
        config = ConfigManager()
    return config

def init_config(config_path: str = "config/config.ini") -> ConfigManager:
    """Initialize the global configuration instance."""
    global config
    config = ConfigManager(config_path)
    return config