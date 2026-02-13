"""
Script to create Elasticsearch indices for the compliance agent.
Run this once to initialize your Elasticsearch cluster.
"""

import json
import os
from pathlib import Path
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# Configure logging
logger.remove()
logger.add(lambda msg: print(msg, end=""), format="{message}")


class ElasticsearchSetup:
    """Setup Elasticsearch indices for compliance tracking."""

    def __init__(self):
        """Initialize Elasticsearch connection."""
        self.es_url = os.getenv("ELASTICSEARCH_URL")
        self.es_api_key = os.getenv("ELASTICSEARCH_API_KEY")

        if not self.es_url or not self.es_api_key:
            raise ValueError(
                "ELASTICSEARCH_URL and ELASTICSEARCH_API_KEY must be set in .env"
            )

        self.es = Elasticsearch(
            [self.es_url],
            api_key=(self.es_api_key.split(":")[0], self.es_api_key.split(":")[1])
            if ":" in self.es_api_key
            else self.es_api_key,
        )

        self.indices_dir = Path(__file__).parent / "indices"

    def test_connection(self) -> bool:
        """Test connection to Elasticsearch."""
        try:
            info = self.es.info()
            logger.info(f"✅ Connected to Elasticsearch {info['version']['number']}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Elasticsearch: {e}")
            return False

    def create_index(self, index_name: str, schema_file: str) -> bool:
        """Create a single index with the given schema."""
        try:
            schema_path = self.indices_dir / schema_file
            if not schema_path.exists():
                logger.error(f"❌ Schema file not found: {schema_path}")
                return False

            with open(schema_path, "r") as f:
                schema = json.load(f)

            # Check if index already exists
            if self.es.indices.exists(index=index_name):
                logger.warning(f"⚠️  Index '{index_name}' already exists, skipping...")
                return True

            # Create the index
            self.es.indices.create(index=index_name, **schema)
            logger.info(f"✅ Created index: {index_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to create index '{index_name}': {e}")
            return False

    def setup_all_indices(self) -> bool:
        """Create all required indices."""
        indices = {
            "regulations": "regulations.json",
            "permits": "permits.json",
            "inspections": "inspections.json",
            "compliance_events": "compliance_events.json",
        }

        logger.info("🚀 Starting Elasticsearch setup...\n")

        if not self.test_connection():
            return False

        logger.info("\n📑 Creating indices...\n")

        all_success = True
        for index_name, schema_file in indices.items():
            if not self.create_index(index_name, schema_file):
                all_success = False

        if all_success:
            logger.info("\n✨ All indices created successfully!")
        else:
            logger.error("\n❌ Some indices failed to create")

        return all_success

    def list_indices(self):
        """List all indices in the cluster."""
        try:
            indices = self.es.indices.get(index="*")
            logger.info("\n📊 Current indices:")
            for index_name in indices.keys():
                logger.info(f"  - {index_name}")
        except Exception as e:
            logger.error(f"Failed to list indices: {e}")

    def delete_all_indices(self):
        """Delete all compliance-related indices (use with caution!)."""
        indices = ["regulations", "permits", "inspections", "compliance_events"]
        try:
            for index_name in indices:
                if self.es.indices.exists(index=index_name):
                    self.es.indices.delete(index=index_name)
                    logger.info(f"✅ Deleted index: {index_name}")
        except Exception as e:
            logger.error(f"Failed to delete indices: {e}")


def main():
    """Main setup function."""
    try:
        setup = ElasticsearchSetup()
        setup.setup_all_indices()
        setup.list_indices()
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()