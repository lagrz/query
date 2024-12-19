import argparse
import logging
import sys
from pathlib import Path

from .queryprocessor import QueryProcessor  # Added missing import
from .utils import get_template_environment, parse_initial_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def write_output(content: str, output_path: Path) -> None:
    """Write content to the specified output file, creating directories if needed."""
    try:
        # Create parent directories if they don't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the content to the file
        output_path.write_text(content)
        logger.info(f"Output written to: {output_path}")
    except Exception as e:
        logger.error(f"Error writing to output file: {e}")
        raise


def cli() -> None:
    parser = argparse.ArgumentParser(
        description="Process queries from YAML configuration"
    )
    parser.add_argument(
        "--file", type=Path, required=True, help="Path to YAML configuration file"
    )
    parser.add_argument(
        "--initial_data", type=str, help="Initial data in key=value,key2=value2 format"
    )
    parser.add_argument(
        "--output", "-o", type=Path, help="Path to output file (optional)"
    )

    args = parser.parse_args()

    if not args.file.exists():
        logger.error(f"Configuration file not found: {args.file}")
        sys.exit(1)

    try:
        initial_data = (
            parse_initial_data(args.initial_data) if args.initial_data else {}
        )
        template_env = get_template_environment()
        processor = QueryProcessor(args.file, template_env, initial_data)
        result = processor.process()
        if args.output:
            write_output(result, args.output)
        else:
            print(result)
    except Exception as e:
        logger.error(f"Error processing queries: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
