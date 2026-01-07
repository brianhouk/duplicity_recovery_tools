#!/usr/bin/env python3
"""
Parallel Multi-Volume File Reassembly Tool

Reassembles fragmented files from a multi-volume snapshot directory.
Uses multiprocessing for efficient parallel processing of large files.

Usage:
    ./reassemble_multivol.py <multivol_dir> <output_dir> [options]

Example:
    ./reassemble_multivol.py snapshot-multivol snapshot --workers 4 --cleanup
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from multiprocessing import Pool, cpu_count, Manager
from typing import List, Tuple
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class MultiVolReassembler:
    """Handles reassembly of multi-volume snapshot files"""

    def __init__(self, multivol_dir: Path, output_dir: Path,
                 workers: int = None, chunk_size: int = 8*1024*1024,
                 cleanup: bool = False, dry_run: bool = False):
        self.multivol_dir = Path(multivol_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.workers = workers or max(1, cpu_count() - 1)
        self.chunk_size = chunk_size  # 8MB default for streaming
        self.cleanup = cleanup
        self.dry_run = dry_run

        if not self.multivol_dir.exists():
            raise ValueError(f"Multi-volume directory does not exist: {self.multivol_dir}")

        logger.info(f"Multi-volume directory: {self.multivol_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Workers: {self.workers}")
        logger.info(f"Chunk size: {self.chunk_size:,} bytes")
        logger.info(f"Cleanup after assembly: {self.cleanup}")
        logger.info(f"Dry run: {self.dry_run}")

    def find_leaf_directories(self) -> List[Path]:
        """Find all leaf directories (directories with no subdirectories)"""
        logger.info("Scanning for leaf directories...")
        leaves = []

        for root, dirs, files in os.walk(self.multivol_dir):
            # A leaf directory has files but no subdirectories
            if files and not dirs:
                leaf_path = Path(root)
                # Only include if it has numeric files
                if self._has_numeric_files(leaf_path):
                    leaves.append(leaf_path)

        logger.info(f"Found {len(leaves)} leaf directories to reassemble")
        return leaves

    def _has_numeric_files(self, directory: Path) -> bool:
        """Check if directory contains files with numeric names"""
        try:
            for item in directory.iterdir():
                if item.is_file():
                    try:
                        int(item.name)
                        return True
                    except ValueError:
                        continue
            return False
        except Exception:
            return False

    def get_relative_path(self, leaf_dir: Path) -> Path:
        """Get the relative path from multivol_dir to use as output filename"""
        return leaf_dir.relative_to(self.multivol_dir)

    def reassemble_file(self, leaf_dir: Path, progress_counter=None) -> Tuple[bool, str, Path]:
        """
        Reassemble a single fragmented file from a leaf directory

        Returns:
            (success: bool, message: str, output_path: Path)
        """
        try:
            # Determine output path
            relative_path = self.get_relative_path(leaf_dir)
            output_path = self.output_dir / relative_path

            if self.dry_run:
                # For dry run, count fragments
                fragment_count = sum(1 for _ in self._get_sorted_fragments(leaf_dir))
                return (True, f"DRY RUN: Would assemble {fragment_count} fragments", output_path)

            # Create output directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Assemble file using streaming to handle large files
            # fragments is a generator, consumed during assembly
            fragment_count, bytes_written = self._stream_assemble(
                self._get_sorted_fragments(leaf_dir), output_path
            )

            if fragment_count == 0:
                return (False, f"No numeric fragments found", output_path)

            # Cleanup if requested
            if self.cleanup:
                try:
                    shutil.rmtree(leaf_dir)
                except Exception as e:
                    logger.warning(f"Failed to cleanup {leaf_dir}: {e}")

            # Update progress
            if progress_counter:
                progress_counter.value += 1
                current = progress_counter.value
                total = progress_counter.total
                if current % 10 == 0 or current == total:
                    logger.info(f"Progress: {current}/{total} files reassembled ({100*current/total:.1f}%)")

            return (True, f"Assembled {fragment_count} fragments → {bytes_written:,} bytes", output_path)

        except Exception as e:
            return (False, f"Error: {str(e)}", leaf_dir)

    def _get_sorted_fragments(self, directory: Path):
        """
        Generator that yields fragment files in numeric order.
        Memory-efficient for directories with hundreds of thousands of fragments.
        """
        # First pass: collect (number, filename) tuples (lightweight)
        fragments = []

        for item in directory.iterdir():
            if item.is_file():
                try:
                    # Only include files with numeric names
                    num = int(item.name)
                    # Store just the number and filename string, not Path object
                    fragments.append((num, item.name))
                except ValueError:
                    logger.warning(f"Skipping non-numeric file: {item}")
                    continue

        # Sort by numeric value
        fragments.sort(key=lambda x: x[0])

        # Yield paths one at a time to avoid keeping full list in memory
        for _, filename in fragments:
            yield directory / filename

        # Clean up the fragments list
        del fragments

    def _stream_assemble(self, fragments, output_path: Path) -> tuple:
        """
        Assemble fragments using streaming I/O to handle large files.
        Accepts a generator to minimize memory usage with large fragment counts.

        Args:
            fragments: Generator yielding Path objects for fragments in order
            output_path: Where to write the assembled file

        Returns:
            (fragment_count, bytes_written)
        """
        bytes_written = 0
        fragment_count = 0

        with output_path.open('wb') as output_file:
            for fragment_path in fragments:
                fragment_count += 1
                with fragment_path.open('rb') as fragment_file:
                    while True:
                        chunk = fragment_file.read(self.chunk_size)
                        if not chunk:
                            break
                        output_file.write(chunk)
                        bytes_written += len(chunk)

        return (fragment_count, bytes_written)

    def run(self):
        """Main execution: find leaf directories and reassemble in parallel"""
        # Find all leaf directories
        leaves = self.find_leaf_directories()

        if not leaves:
            logger.warning("No leaf directories found to reassemble")
            return 0

        # Create output directory
        if not self.dry_run:
            self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup progress tracking (only use Manager for multi-worker)
        if self.workers == 1:
            progress = None
        else:
            manager = Manager()
            progress = manager.Namespace()
            progress.value = 0
            progress.total = len(leaves)

        logger.info(f"Starting reassembly with {self.workers} workers...")

        # Process in parallel
        success_count = 0
        error_count = 0

        if self.workers == 1:
            # Single-threaded - process iteratively to avoid memory accumulation
            for idx, leaf in enumerate(leaves, 1):
                success, message, path = self.reassemble_file(leaf, None)

                # Manual progress logging
                if idx % 10 == 0 or idx == len(leaves):
                    logger.info(f"Progress: {idx}/{len(leaves)} files reassembled ({100*idx/len(leaves):.1f}%)")
                if success:
                    success_count += 1
                    logger.debug(f"✓ {path.name}: {message}")
                else:
                    error_count += 1
                    logger.error(f"✗ {path}: {message}")
        else:
            # Multi-threaded processing
            with Pool(processes=self.workers) as pool:
                # Use starmap to pass both leaf and progress counter
                results = pool.starmap(
                    self.reassemble_file,
                    [(leaf, progress) for leaf in leaves]
                )

            # Process results
            for success, message, path in results:
                if success:
                    success_count += 1
                    logger.debug(f"✓ {path.name}: {message}")
                else:
                    error_count += 1
                    logger.error(f"✗ {path}: {message}")

        # Summary
        logger.info("=" * 60)
        logger.info(f"Reassembly complete!")
        logger.info(f"  Success: {success_count}")
        logger.info(f"  Errors:  {error_count}")
        logger.info(f"  Total:   {len(leaves)}")
        logger.info("=" * 60)

        return error_count


def main():
    parser = argparse.ArgumentParser(
        description="Reassemble multi-volume snapshot files in parallel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Reassemble with default workers (CPU count - 1)
  %(prog)s snapshot-multivol snapshot

  # Use 8 workers and cleanup multivol directories after success
  %(prog)s snapshot-multivol snapshot --workers 8 --cleanup

  # Dry run to see what would be assembled
  %(prog)s snapshot-multivol snapshot --dry-run

  # Verbose logging
  %(prog)s snapshot-multivol snapshot -v
        """
    )

    parser.add_argument(
        'multivol_dir',
        type=str,
        help='Directory containing multi-volume fragmented files'
    )

    parser.add_argument(
        'output_dir',
        type=str,
        help='Directory where reassembled files will be written'
    )

    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=None,
        help='Number of parallel workers (default: CPU count - 1)'
    )

    parser.add_argument(
        '-c', '--chunk-size',
        type=int,
        default=8*1024*1024,
        help='Chunk size for streaming I/O in bytes (default: 8MB)'
    )

    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Delete multivol directories after successful reassembly'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without actually assembling files'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose debug logging'
    )

    args = parser.parse_args()

    # Adjust logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    try:
        reassembler = MultiVolReassembler(
            multivol_dir=args.multivol_dir,
            output_dir=args.output_dir,
            workers=args.workers,
            chunk_size=args.chunk_size,
            cleanup=args.cleanup,
            dry_run=args.dry_run
        )

        exit_code = reassembler.run()
        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.error("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
