require 'functions_framework'
require 'google/cloud/pubsub'
require 'json'
require 'logger'

# Configure the logger
logger = Logger.new(STDOUT)
logger.level = Logger::INFO

# Initialize Pub/Sub client
pubsub = Google::Cloud::PubSub.new

# Configuration
# RESPONSE_TOPIC_NAME = 'trips'  # Topic to send responses back to

FunctionsFramework.http 'hello_world_handler' do |request|
#   # Set CORS headers
#   response_headers = {
#     'Access-Control-Allow-Origin' => '*',
#     'Access-Control-Allow-Methods' => 'GET, POST, OPTIONS',
#     'Access-Control-Allow-Headers' => 'Content-Type',
#     'Content-Type' => 'application/json'
#   }

#   # Handle preflight requests
#   if request.method == 'OPTIONS'
#     return [200, response_headers, '']
#   end

  begin
    # Parse the request body
    request_body = request.body.read
    logger.info("Received request body: #{request_body}")

    # Parse JSON data
    data = JSON.parse(request_body)
    logger.info("Parsed data: #{data}")

    # Extract message data from Pub/Sub message
    if data['message'] && data['message']['data']
      # Decode base64 data if present
      message_data = data['message']['data']
      if data['message']['data'].is_a?(String)
        # Try to decode base64
        begin
          decoded_data = Base64.decode64(data['message']['data'])
          message_data = JSON.parse(decoded_data)
        rescue
          # If not base64, try to parse as JSON directly
          message_data = JSON.parse(data['message']['data'])
        end
      end
    else
      message_data = data
    end

    logger.info("Processing message: #{message_data}")

    # Handle different actions
    case message_data['action']
    when 'hello_world'
      result = handle_hello_world(message_data, pubsub, logger)
    when 'simulate'
      result = handle_simulation(message_data, pubsub, logger)
    else
      result = { error: "Unknown action: #{message_data['action']}" }
    end

    # Return success response
    # [200, response_headers, result.to_json]

  rescue => e
    logger.error("Error processing request: #{e.message}")
    logger.error(e.backtrace.join("\n"))
    
    error_response = {
      error: e.message,
      timestamp: Time.now.iso8601
    }
    
    # [500, response_headers, error_response.to_json]
  end
end

# Handle Hello World messages
def handle_hello_world(message_data, pubsub, logger)
  message_number = message_data['message_number']
  original_message = message_data['message']
  
  logger.info("Processing Hello World message #{message_number}")
  
  # Simulate some processing time (optional)
    1_000_000_000.times.each {}
    logger.info("Sorry, I was sleeping #{message_number}")

  
  # Create response message
  response_data = {
    message: "Hello back from cloud function! Processed message #{message_number}",
    original_message: original_message,
    original_message_number: message_number,
    processed_at: Time.now.iso8601,
    status: 'success',
    thread_id: message_data['thread_id'] || 'unknown',
    processing_time_ms: rand(50..200)  # Simulate processing time
  }
  
  # Send response back to the response topic
  send_response(response_data, pubsub, logger)
  
  # Return success result
  {
    status: 'success',
    message_number: message_number,
    processed_at: Time.now.iso8601,
    response_sent: true
  }
end

# Handle simulation messages (for future use)
def handle_simulation(message_data, pubsub, logger)
  logger.info("Processing simulation message")
  
  # Create response for simulation
  response_data = {
    message: "Simulation processed successfully",
    original_data: message_data,
    processed_at: Time.now.iso8601,
    status: 'success',
    simulation_id: SecureRandom.uuid
  }
  
  # Send response back
  send_response(response_data, pubsub, logger)
  
  {
    status: 'success',
    simulation_processed: true,
    processed_at: Time.now.iso8601
  }
end

# Send response to the response topic
def send_response(response_data, pubsub, logger)
  begin
    # Get or create the response topic
    response_topic = pubsub.topic(RESPONSE_TOPIC_NAME)
    
    # Publish the response
    message = response_topic.publish(response_data.to_json)
    
    logger.info("Response sent to topic #{RESPONSE_TOPIC_NAME}: #{message.message_id}")
    
    # Add message ID to response data
    response_data[:response_message_id] = message.message_id
    
  rescue => e
    logger.error("Failed to send response: #{e.message}")
    response_data[:response_error] = e.message
  end
end

# Helper function to generate UUID
