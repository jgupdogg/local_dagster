"""Slack notification resource for Dagster pipelines."""
from dagster import resource
from dagster_slack import SlackResource
import os
import logging

logger = logging.getLogger(__name__)

@resource
def slack_resource(context):
    """Resource for sending Slack notifications via the official Slack API."""
    # Use the Bot User OAuth Token (xoxb-) with chat:write scope
    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    
    if not slack_token:
        logger.warning("SLACK_BOT_TOKEN not found in environment variables.")
        return None
    
    # Create and return the official SlackResource
    return SlackResource(token=slack_token)