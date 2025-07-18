"""
GCP Secret Manager Integration for AI Trading Machine
====================================================

This module provides secure credential management using GCP Secret Manager.
It replaces environment variables and .env files for production deployments.

Features:
- Secure storage and retrieval of API credentials
- Automatic credential rotation support
- Audit logging for secret access
- Environment-based secret management

Dependencies:
- google-cloud-secret-manager
- google-auth

Usage:
    from ai_trading_machine.utils.gcp_secrets import SecretManager

    secret_mgr = SecretManager()
    api_key = secret_mgr.get_secret("kite-api-key")

Author: AI Trading Machine
Licensed by SJ Trading
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Optional

try:
    from google.auth import default
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import secretmanager

    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False
    logging.warning(
        "GCP Secret Manager not available. Install with: pip install google-cloud-secret-manager"
    )

from .logger import setup_logger

logger = setup_logger(__name__)


class SecretManager:
    """
    GCP Secret Manager client for secure credential management.
    """

    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize Secret Manager client.

        Args:
            project_id: GCP project ID (optional, auto-detected if not provided)
        """
        if not GCP_AVAILABLE:
            raise ImportError(
                "GCP Secret Manager not available. Install google-cloud-secret-manager"
            )

        self.project_id = project_id or self._get_project_id()

        try:
            # Initialize the Secret Manager client
            self.client = secretmanager.SecretManagerServiceClient()
            logger.info("Secret Manager initialized for project: {self.project_id}")
        except DefaultCredentialsError:
            logger.error("GCP credentials not found. Ensure you're authenticated.")
            raise
        except Exception as e:
            logger.error("Failed to initialize Secret Manager: {e}")
            raise

    def _get_project_id(self) -> str:
        """Auto-detect GCP project ID."""
        try:
            # Try environment variable first
            project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
            if project_id:
                return project_id

            # Try to get from default credentials
            _, project_id = default()
            if project_id:
                return project_id

            # Fallback to metadata service (if running on GCP)
            import requests

            response = requests.get(
                "http://metadata.google.internal/computeMetadata/v1/project/project-id",
                headers={"Metadata-Flavor": "Google"},
                timeout=1,
            )
            if response.status_code == 200:
                return response.text

        except Exception as e:
            logger.debug("Could not auto-detect project ID: {e}")

        raise ValueError(
            "Could not determine GCP project ID. Please set GOOGLE_CLOUD_PROJECT or pass project_id"
        )

    def get_secret(self, secret_id: str, version: str = "latest") -> Optional[str]:
        """
        Retrieve a secret from Secret Manager.

        Args:
            secret_id: Secret ID (e.g., "kite-api-key")
            version: Secret version ("latest" or specific version number)

        Returns:
            Secret value as string, or None if not found
        """
        try:
            # Build the resource name
            name = "projects/{self.project_id}/secrets/{secret_id}/versions/{version}"

            # Access the secret version
            response = self.client.access_secret_version(request={"name": name})

            # Decode the secret payload
            secret_value = response.payload.data.decode("UTF-8")

            logger.debug("Successfully retrieved secret: {secret_id}")
            return secret_value

        except Exception as e:
            logger.error("Failed to retrieve secret {secret_id}: {e}")
            return None

    def create_secret(
        self, secret_id: str, secret_value: str, labels: Optional[dict[str, str]] = None
    ) -> bool:
        """
        Create a new secret in Secret Manager.

        Args:
            secret_id: Secret ID
            secret_value: Secret value
            labels: Optional labels for the secret

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build the resource name
            parent = "projects/{self.project_id}"

            # Create the secret
            secret_request = {
                "parent": parent,
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}, "labels": labels or {}},
            }

            secret = self.client.create_secret(request=secret_request)
            logger.info("Created secret: {secret.name}")

            # Add the secret version with the actual value
            version_request = {
                "parent": secret.name,
                "payload": {"data": secret_value.encode("UTF-8")},
            }

            version = self.client.add_secret_version(request=version_request)
            logger.info("Added secret version: {version.name}")

            return True

        except Exception as e:
            logger.error("Failed to create secret {secret_id}: {e}")
            return False

    def update_secret(self, secret_id: str, secret_value: str) -> bool:
        """
        Update an existing secret with a new value.

        Args:
            secret_id: Secret ID
            secret_value: New secret value

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build the resource name
            parent = "projects/{self.project_id}/secrets/{secret_id}"

            # Add new secret version
            request = {
                "parent": parent,
                "payload": {"data": secret_value.encode("UTF-8")},
            }

            version = self.client.add_secret_version(request=request)
            logger.info("Updated secret {secret_id} with new version: {version.name}")

            return True

        except Exception as e:
            logger.error("Failed to update secret {secret_id}: {e}")
            return False

    def delete_secret(self, secret_id: str) -> bool:
        """
        Delete a secret from Secret Manager.

        Args:
            secret_id: Secret ID

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build the resource name
            name = "projects/{self.project_id}/secrets/{secret_id}"

            # Delete the secret
            self.client.delete_secret(request={"name": name})
            logger.info("Deleted secret: {secret_id}")

            return True

        except Exception as e:
            logger.error("Failed to delete secret {secret_id}: {e}")
            return False

    def list_secrets(
        self, filter_str: Optional[str] = None
    ) -> dict[str, dict[str, Any]]:
        """
        List all secrets in the project.

        Args:
            filter_str: Optional filter string (e.g., "labels.app=ai-trading-machine")

        Returns:
            Dictionary of secret information
        """
        try:
            # Build the resource name
            parent = "projects/{self.project_id}"

            # List secrets
            request = {"parent": parent}
            if filter_str:
                request["filter"] = filter_str

            secrets = {}
            for secret in self.client.list_secrets(request=request):
                secret_info = {
                    "name": secret.name.split("/")[-1],
                    "create_time": secret.create_time,
                    "labels": dict(secret.labels) if secret.labels else {},
                    "replication": str(secret.replication),
                }
                secrets[secret_info["name"]] = secret_info

            logger.debug("Listed {len(secrets)} secrets")
            return secrets

        except Exception as e:
            logger.error("Failed to list secrets: {e}")
            return {}


class TradingSecrets:
    """
    High-level interface for trading-specific secrets management.
    """

    def __init__(self, secret_manager: Optional[SecretManager] = None):
        """
        Initialize trading secrets manager.

        Args:
            secret_manager: Optional SecretManager instance
        """
        self.secret_manager = secret_manager or SecretManager()

        # Define secret mappings
        self.secret_mappings = {
            "KITE_API_KEY": "kite-api-key",
            "KITE_API_SECRET": "kite-api-secret",
            "KITE_ACCESS_TOKEN": "kite-access-token",
            "TRADING_CONFIG": "trading-config",
            "DB_CONNECTION_STRING": "db-connection-string",
        }

    def get_kite_credentials(self) -> dict[str, Optional[str]]:
        """
        Get all KiteConnect credentials.

        Returns:
            Dictionary with API key, secret, and access token
        """
        credentials = {}

        for env_var, secret_id in self.secret_mappings.items():
            if env_var.startswith("KITE_"):
                credentials[env_var] = self.secret_manager.get_secret(secret_id)

        return credentials

    def update_access_token(self, access_token: str) -> bool:
        """
        Update the KiteConnect access token.

        Args:
            access_token: New access token

        Returns:
            True if successful, False otherwise
        """
        return self.secret_manager.update_secret("kite-access-token", access_token)

    def get_trading_config(self) -> Optional[dict[str, Any]]:
        """
        Get trading configuration from secrets.

        Returns:
            Trading configuration as dictionary
        """
        config_json = self.secret_manager.get_secret("trading-config")
        if config_json:
            try:
                return json.loads(config_json)
            except json.JSONDecodeError as e:
                logger.error("Failed to parse trading config JSON: {e}")

        return None

    def setup_initial_secrets(self, credentials: dict[str, str]) -> bool:
        """
        Set up initial secrets for the trading system.

        Args:
            credentials: Dictionary of credential values

        Returns:
            True if all secrets created successfully
        """
        success = True

        for env_var, secret_id in self.secret_mappings.items():
            if env_var in credentials:
                labels = {
                    "app": "ai-trading-machine",
                    "type": "api-credential" if "KITE_" in env_var else "configuration",
                    "created": datetime.now().isoformat(),
                }

                result = self.secret_manager.create_secret(
                    secret_id, credentials[env_var], labels
                )

                if not result:
                    success = False
                    logger.error("Failed to create secret for {env_var}")

        return success


def get_secret_or_env(
    secret_id: str, env_var: str, fallback_to_env: bool = True
) -> Optional[str]:
    """
    Get secret from Secret Manager with environment variable fallback.

    This function provides a smooth transition from environment variables
    to Secret Manager for existing deployments.

    Args:
        secret_id: Secret Manager secret ID
        env_var: Environment variable name
        fallback_to_env: Whether to fallback to environment variable

    Returns:
        Secret value or None
    """
    try:
        # Try Secret Manager first
        secret_mgr = SecretManager()
        value = secret_mgr.get_secret(secret_id)

        if value:
            logger.debug("Retrieved {secret_id} from Secret Manager")
            return value

    except Exception as e:
        logger.debug("Could not retrieve from Secret Manager: {e}")

    # Fallback to environment variable
    if fallback_to_env:
        value = os.getenv(env_var)
        if value:
            logger.debug("Retrieved {env_var} from environment variable")
            return value

    logger.warning("Could not retrieve secret {secret_id} or env var {env_var}")
    return None


# Convenience functions for common secrets
def get_kite_api_key() -> Optional[str]:
    """Get KiteConnect API key from Secret Manager or environment."""
    return get_secret_or_env("kite-api-key", "KITE_API_KEY")


def get_kite_api_secret() -> Optional[str]:
    """Get KiteConnect API secret from Secret Manager or environment."""
    return get_secret_or_env("kite-api-secret", "KITE_API_SECRET")


def get_kite_access_token() -> Optional[str]:
    """Get KiteConnect access token from Secret Manager or environment."""
    return get_secret_or_env("kite-access-token", "KITE_ACCESS_TOKEN")
