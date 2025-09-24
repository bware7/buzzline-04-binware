"""
project_consumer_binware.py

A simple consumer that reads JSON messages from Kafka and visualizes
sentiment trends by category in real-time.

"""

import json
import os
import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv

# Import Kafka only if available
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("Kafka not available")

# Import logging utility
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Get Environment Variables
#####################################

def get_kafka_topic():
    return os.getenv("PROJECT_TOPIC", "project_json")

def get_kafka_server():
    return os.getenv("KAFKA_SERVER", "localhost:9092")

#####################################
# Consumer Class
#####################################

class SentimentConsumer:
    def __init__(self):
        """Initialize the sentiment consumer."""
        self.topic = get_kafka_topic()
        self.server = get_kafka_server()
        
        # Data storage - keep sentiment values by category
        self.category_sentiments = defaultdict(list)
        self.message_numbers = defaultdict(list)
        
        # Keep only recent data points
        self.max_points = 50
        
        # Message counter
        self.message_count = 0
        
        # Setup Kafka consumer
        self.setup_kafka()
        
        # Setup the plot
        self.setup_plot()
        
        # Track if consumer is running
        self.running = True
    
    def setup_kafka(self):
        """Setup Kafka consumer."""
        if not KAFKA_AVAILABLE:
            raise Exception("Kafka not available - install kafka-python")
        
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[self.server],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='sentiment-consumer-binware',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000  # Timeout after 1 second
            )
            logger.info(f"Connected to Kafka topic: {self.topic}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def setup_plot(self):
        """Setup the matplotlib plot."""
        self.fig, self.ax = plt.subplots(figsize=(12, 6))
        self.ax.set_title('Sentiment by Category Over Time', fontsize=14)
        self.ax.set_xlabel('Message Number')
        self.ax.set_ylabel('Sentiment Score')
        self.ax.set_ylim(0, 1)
        self.ax.grid(True, alpha=0.3)
        
        # Colors for different categories
        self.colors = {
            'humor': 'red',
            'tech': 'blue', 
            'food': 'green',
            'travel': 'orange',
            'entertainment': 'purple',
            'gaming': 'brown',
            'other': 'gray'
        }
    
    def process_message(self, message):
        """Process a single message."""
        try:
            # Get the data we need
            sentiment = message.get('sentiment', 0.0)
            category = message.get('category', 'other')
            timestamp = message.get('timestamp', '')
            
            # Store the data
            self.category_sentiments[category].append(sentiment)
            self.message_numbers[category].append(self.message_count)
            
            # Keep only recent points
            if len(self.category_sentiments[category]) > self.max_points:
                self.category_sentiments[category].pop(0)
                self.message_numbers[category].pop(0)
            
            self.message_count += 1
            
            logger.info(f"Message {self.message_count}: {category} = {sentiment}")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def update_chart(self, frame):
        """Update the chart with new data."""
        if not self.running:
            return []
        
        try:
            # Get new messages with timeout
            messages = self.consumer.poll(timeout_ms=100)
            
            # Process new messages
            for topic_partition, message_list in messages.items():
                for message in message_list:
                    self.process_message(message.value)
        except Exception as e:
            logger.error(f"Error polling messages: {e}")
            # Continue anyway
        
        # Clear and redraw
        self.ax.clear()
        self.ax.set_title(f'Sentiment by Category Over Time (Messages: {self.message_count})', fontsize=14)
        self.ax.set_xlabel('Message Number')
        self.ax.set_ylabel('Sentiment Score')
        self.ax.set_ylim(0, 1)
        self.ax.grid(True, alpha=0.3)
        
        # Plot each category
        for category, sentiments in self.category_sentiments.items():
            if sentiments:  # Only plot if we have data
                message_nums = self.message_numbers[category]
                color = self.colors.get(category, 'gray')
                self.ax.plot(message_nums, sentiments, 
                           color=color, marker='o', markersize=4, 
                           label=f'{category} ({len(sentiments)})',
                           linewidth=2)
        
        # Add legend if we have data
        if self.category_sentiments:
            self.ax.legend(loc='upper right')
        
        return []
    
    def start_consuming(self):
        """Start consuming messages and updating the chart."""
        logger.info("Starting sentiment consumer...")
        logger.info("Press Ctrl+C to stop")
        
        try:
            # Create animation
            anim = animation.FuncAnimation(
                self.fig,
                self.update_chart,
                interval=1000,  # Update every 1 second
                blit=False,
                cache_frame_data=False
            )
            
            plt.tight_layout()
            plt.show()
            
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        self.running = False
        try:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")

#####################################
# Main Function
#####################################

def main():
    """Main function."""
    try:
        consumer = SentimentConsumer()
        consumer.start_consuming()
    except Exception as e:
        logger.error(f"Failed to start consumer: {e}")
        print(f"Error: {e}")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()